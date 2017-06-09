(ns pythia.rdf
  (:import [org.apache.jena.rdf.model ModelFactory]))

(defn node->triple [node]
  (let [s (.getLocalName (.getSubject node))
        p (.getLocalName (.getPredicate node))
        o (.getObject node)
        o (if (.isLiteral o)
            (.getValue o)
            (.getLocalName o))]
    [s p o]))

(defn read-rdf-file [filename]
  (let [model (ModelFactory/createDefaultModel)]
    (with-open [in (clojure.java.io/input-stream filename)]
      (.read model in nil)
      (map node->triple (iterator-seq (.listStatements model))))))

;; (def uni (read-rdf-file "University0_0.owl"))

