(ns com.geos.kafka-pedestal.consumer
  "Kafka consumer adapter wich provides a polling in loop to dispatch messages to the pedestal interceptors chain.
   It also provides a MockConsumer with some functions to help tests."
  (:require [cheshire.core :as json]
            [clojure.java.io :as jio]
            [clojure.stacktrace :as stacktrace]
            [clojure.string :as str]
            [io.pedestal.log :as logger])
  (:import [java.time Duration]
           [java.util Properties]
           [org.apache.kafka.clients.consumer
            ConsumerConfig
            ConsumerRecord
            ConsumerRecords
            KafkaConsumer
            MockConsumer
            OffsetResetStrategy]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.config SaslConfigs]
           [org.apache.kafka.common.errors WakeupException]))

(defn test-service? [service-map]
  (= :test (:env service-map)))

(defn- service-map->properties [service-map]
  (let [{config-fname ::config.file
         bootstrap-servers ::bootstrap.servers
         username ::username
         password ::password
         client-id ::client.id
         group-id ::group.id
         max-poll-records ::max.poll.records
         auto-offset-reset ::auto.offset.reset} service-map]

    (with-open [config-file (jio/reader config-fname)]
      (doto (Properties.)
        (.putAll
         {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        bootstrap-servers
          SaslConfigs/SASL_JAAS_CONFIG                   (str/join " " ["org.apache.kafka.common.security.plain.PlainLoginModule" "required"
                                                                        (str "username='" username "'")
                                                                        (str "password='" password "';")])
          ConsumerConfig/CLIENT_ID_CONFIG                client-id
          ConsumerConfig/GROUP_ID_CONFIG                 group-id
          ConsumerConfig/AUTO_OFFSET_RESET_CONFIG        (when auto-offset-reset (str auto-offset-reset))
          ConsumerConfig/MAX_POLL_RECORDS_CONFIG         (when max-poll-records (Integer. max-poll-records))
          ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringDeserializer"
          ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
          ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG       "false"})
        (.load config-file)))))

(defn- create-consumer [service-map]
  (if (test-service? service-map)
    (-> service-map
        (assoc ::client-id (::client.id service-map))
        (assoc ::consumer (MockConsumer. OffsetResetStrategy/EARLIEST)))

    (let [props (service-map->properties service-map)
          client-id (.getProperty props "client.id")]
      (-> service-map
          (assoc ::client-id client-id)
          (assoc ::consumer (KafkaConsumer. props))
          ;; Discard kafka credentials values from service-map
          (dissoc ::config.file ::bootstrap.servers ::username ::password)))))

(defn- pool-and-dispatch [consumer client-id topic dispatcher-fn ^Duration duration]
  (logger/debug :polling-messages client-id :topic topic)

  (let [^ConsumerRecords msgs (.poll consumer duration)]
    (when (not (.isEmpty msgs))
      (logger/info :received-messages client-id :topic topic :count (.count msgs))
      (dispatcher-fn client-id consumer msgs))))

(defn- fetch-committed-msgs [service-map]
  (let [consumer (::consumer service-map)]
    (.committed consumer (.assignment consumer))))

(defn- start-loop [consumer client-id topic dispatcher-fn service-map duration]
  (let [continue? (atom true)
        last-committed (when (test-service? service-map)
                         (atom (java.util.Collections/singletonMap (TopicPartition. "" 0) 0)))
        last-exception (when (test-service? service-map)
                         (atom (RuntimeException.)))
        completion (future
                     (try
                       (while @continue?
                         (try
                           (pool-and-dispatch consumer client-id topic dispatcher-fn duration)
                           (catch WakeupException _)))
                       :ok
                       (catch Throwable t
                         (when (test-service? service-map)
                           (reset! last-exception t))

                         (logger/error :msg "Dispatch code threw an exception"
                                       :cause t
                                       :cause-trace (with-out-str
                                                      (stacktrace/print-cause-trace t))))

                       (finally
                         (when (test-service? service-map)
                           (reset! last-committed (fetch-committed-msgs service-map)))

                         (.close consumer)
                         (logger/info :stopped :kafka-consumer :client-id client-id))))]

    {::continue? continue?
     ::completion completion
     ::last-committed last-committed
     ::last-exception last-exception}))

(def ^:private test-partition 0)
(def ^:private test-key "TEST")

(defn- init-mock [consumer topic]
  (.rebalance consumer (java.util.Collections/singletonList (TopicPartition. topic test-partition)))
  (.updateBeginningOffsets consumer (java.util.Collections/singletonMap (TopicPartition. topic test-partition) 0)))

(defn- start [service-map]
  (let [{consumer ::consumer
         client-id ::client-id
         topic ::topic
         dispatcher-fn ::dispatcher-fn
         duration ::poll-interval
         :or {duration (Duration/ofSeconds 5)}}
        service-map]

    (.subscribe consumer [topic])
    (logger/info :started :kafka-consumer
                 :client-id client-id
                 :topic topic)

    (when (test-service? service-map)
      (init-mock consumer topic))

    (start-loop consumer client-id topic dispatcher-fn service-map duration)))

(defn- stop [service-map]
  (let [{consumer ::consumer
         continue? ::continue?
         completion ::completion} service-map]

    (when continue?
      (reset! continue? false)
      (.wakeup consumer)
      (deref completion 1000 :timeout))))

(defn- test-msg [service-map data]
  (let [{consumer ::consumer
         topic ::topic} service-map
        offsetPosition (.position consumer (TopicPartition. topic test-partition))]

    (.addRecord consumer (ConsumerRecord. topic test-partition offsetPosition test-key (json/encode data)))))

(defn- check-committed [service-map]
  (deref (::last-committed service-map)))

(defn- check-exception [service-map]
  (deref (::last-exception service-map)))

(defn create
  [service-map]
  (let [consumer (create-consumer service-map)]
    (-> consumer
        (assoc ::start-fn (fn [s] (start s)))
        (assoc ::stop-fn  (fn [s] (stop s)))
        (cond->
         (test-service? service-map) (assoc ::test-msg-fn (fn [s d] (test-msg s d))
                                            ::check-committed (fn [s] (check-committed s))
                                            ::check-exception (fn [s] (check-exception s)))))))
