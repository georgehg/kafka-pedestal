(ns com.geos.kafka-pedestal.consumer-test
  {:clj-kondo/config '{:linters {:unresolved-symbol {:exclude [sut]}
                                 :unused-binding {:exclude [body]}}}}
  (:require [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [com.geos.kafka-pedestal.consumer :as consumer]
            [com.geos.kafka-pedestal.provider :as kafka])
  (:import [org.apache.kafka.clients.consumer MockConsumer]))

(defrecord TestEventsProcessor [processor interceptors]
  component/Lifecycle
  (start [this]
    (if-not processor
      (let [service-map {:env :test
                         ::consumer/topic "topic_test"
                         ::consumer/client.id "test_client"
                         ::kafka/interceptors interceptors}
            processor (-> service-map
                          kafka/create-processor
                          kafka/start-consumer)]

        (assoc this :processor processor))
      this))

  (stop [this]
    (do (cond
          processor (kafka/stop-consumer processor))
        (dissoc this :processor))))

(defn- test-system [interceptors]
  (component/system-map
   :processor (map->TestEventsProcessor {:interceptors interceptors})))

(defmacro with-system
  [[bound-var binding-expr] & body]
  `(let [~bound-var (component/start ~binding-expr)]
     (try
       ~@body
       (finally
         (component/stop ~bound-var)))))

(defn- processor [system]
  (get-in system [:processor :processor]))

(defn- send-message [system data]
  (kafka/test-messsage (processor system) data))

(defn- fetch-error-details [system]
  (:error-details (ex-data (ex-cause (kafka/test-get-last-exception (processor system))))))

(deftest kaka-consumer-tests
  (with-system [sut (test-system [])]

    (testing "Create processor and start consumer"
      (let [processor (processor sut)
            {consumer ::consumer/consumer
             dispatcher ::consumer/dispatcher-fn
             testfn ::consumer/test-msg-fn} processor]
        (is (instance? MockConsumer consumer))
        (is (not (nil? dispatcher)))
        (is (not (nil? testfn)))))

    (testing "Send message and successfully commit kafka record"
      (send-message sut {:user "Joao" :product "Livro" :qty 1})
      (Thread/sleep 1000)
      (is (= 1 (.size (kafka/test-check-committed-msgs (processor sut))))))))

(def raise-error-interceptor
  {:name :raise-error-interceptor
   :enter (fn [ctx]
            (throw (ex-info "Message processing service errors"
                            {:context ctx
                             :error-details {:message "Processing errors"}})))})

(deftest kaka-consumer-tests-with-errors
  (with-system [sut (test-system [raise-error-interceptor])]

    (testing "Add a customs interceptors, send message, raise error and do not commit kafka record"
      (send-message sut {:user "Maria" :product "filme" :qty 10})
      (Thread/sleep 1000)
      (is (= 0 (.size (kafka/test-check-committed-msgs (processor sut)))))
      (is (= {:message "Processing errors"} (fetch-error-details sut))))))
