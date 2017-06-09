(def project 'query)
(def version "0.1.0-SNAPSHOT")

(set-env! :resource-paths #{"resources" "src"}
          :source-paths   #{"test"}
          :dependencies   '[[org.clojure/clojure "RELEASE"]
                            [adzerk/boot-test "RELEASE" :scope "test"]
                            [org.apache.jena/jena-core "RELEASE"]])

(task-options!
 pom {:project     project
      :version     version
      :description "Experimenting with creating a simple database"
      :url         "https://github.com/hencq/pythia.git"
      :scm         {:url "https://github.com/yourname/query"}
      :license     {"Eclipse Public License"
                    "http://www.eclipse.org/legal/epl-v10.html"}})

(deftask build
  "Build and install the project locally."
  []
  (comp (pom) (jar) (install)))

(require '[adzerk.boot-test :refer [test]])
