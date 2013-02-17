(ns pbf.worker)
(import java.util.concurrent.Semaphore)

(defn create-woker
  ([]
     (create-woker (.availableProcessors (Runtime/getRuntime))))
  ([limit]
     (let [semaphore (Semaphore. limit)]
       (fn [work]
         (.acquire semaphore)
         (try
           (future (try (work)
                        (finally (.release semaphore))))
           (catch Exception e
             (.release semaphore)
             (throw e)))))))
