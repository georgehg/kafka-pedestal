(ns com.geos.kafka-pedestal.provider
  "Namespace which ties kafka consumer, messages dispatcher with the pedestal interceptors queue
  using pedestal chain provider to make a kafka consumer application."
  (:require [cheshire.core :as json]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as chain]
            [io.pedestal.log :as logger]
            [com.geos.kafka-pedestal.consumer :as consumer]
            [com.geos.kafka-pedestal.dispatcher :as dispatcher]))

(def log-message
  "Log the message being processed"
  (interceptor/interceptor
   {:name ::log-message
    :enter (fn [context]
             (let [message (:message context)
                   client-id (:client-id context)]
               (logger/debug :message (:value message))
               (logger/info :execution-id (::chain/execution-id context)
                            :message-from-consumer client-id
                            :topic (:topic message)
                            :message-id (-> message :value :message-id)
                            :io.pedestal.log/formatter json/generate-string))
             context)
    :leave (fn [context]
             (logger/info :execution-id (::chain/execution-id context)
                          :result (:result context)
                          :io.pedestal.log/formatter json/generate-string)
             context)}))

(defn- expand-interceptors
  "Valiates and produces Interceptor Records bases on given values
   matching io.pedestal.interceptor.Interceptor type"
  [interceptors]
  (reduce (fn [expanded-interceptors interceptor]
            (concat expanded-interceptors
                    (if (interceptor/interceptor? interceptor)
                      [interceptor]
                      [(interceptor/interceptor interceptor)])))
          []
          interceptors))

(defn default-interceptors
  [service-map]
  (let [{interceptors ::interceptors
         message-logger ::message-logger
         :or {message-logger log-message}} service-map]

    (if interceptors
      (assoc service-map ::interceptors (concat [message-logger] (expand-interceptors interceptors)))
      (assoc service-map ::interceptors [message-logger]))))

(defn- dispatcher-fn [{interceptors ::interceptors :as service-map}]
  (assoc service-map ::consumer/dispatcher-fn
         (dispatcher/kafka-interceptor-dispatcher-fn interceptors)))

(defn- create-provider
  "Creates the base Interceptor Chain provider for Kafka consumer, connecting a message dispatcher
   to the interceptor chain."
  [service-map]
  (-> service-map
      default-interceptors
      dispatcher-fn))

(defn- consumer
  [service-map]
  (consumer/create service-map))

(defn create-processor
  "Given a service map, creates an returns an initialized service map which is
  ready to be started via `kafka/start-consumer`.

  Notes:
  - The returned, initialized service map contains the `::consumer/start-fn`
    and `::consumer/stop-fn` keys whose values are functions with are used
    to start/stop the http service, respectively. These functions are executed
    with the updated service-map as arg containing the handlers for the kaka consumer loop.
  - The resulting service-map will contain the `::consumer/test-msg-fn key if in :test mode
    which is useful for testing services sendig messages to the Kaka MockConsumer."
  [service-map]
  (-> service-map
      create-provider
      consumer))

(defn start-consumer
  [service-map]
  (merge service-map ((::consumer/start-fn service-map) service-map)))

(defn stop-consumer
  [service-map]
  ((::consumer/stop-fn service-map) service-map))

(defn test-messsage
  [service-map data]
  ((::consumer/test-msg-fn service-map) service-map data))

(defn test-check-committed-msgs
  [service-map]
  ((::consumer/check-committed service-map) service-map))

(defn test-get-last-exception
  [service-map]
  ((::consumer/check-exception service-map) service-map))
