#!/usr/bin/env groovy

@Library("osiris-python-module@release/python39") l1
@Library("osiris-moma-module") l2
@Library("sonar@lts") l3

node("spark-python39") {
    try {
        stage("Checkout") {
            checkout scm
        }

        stage("Python Worker") {
            if (!python.isBuildable()) {
                return true
            }

            def isNotebook = fileExists '.notebook'
            def isMoma = fileExists '.moma'
            def isDataflow = fileExists 'exampleenginepythonqiyhbwvw/dataflow.py'
            def adapter = isNotebook ? python.&notebook : python.&worker;

            // Some adapter methods could not exist, in this case Osiris will print
            // a log and then the pipeline continues executing the next line.

            if (isNotebook || isMoma || isDataflow) {
                moma.developToMaster()
            } else {
                python("samuelPreBuild")
            }

            adapter("verify")
            python("venv")
            adapter("requirementsAdd")
            adapter("export",["experiment.py","notebook31"])
            adapter("containerBuild")
            adapter("containerSetup")

            parallel(
                containerTest: {
                    adapter("containerTest")
                },
                containerPackage: {
                    adapter("containerPackage")
                },
                sonar: {
                    python("sonar")
                }
            )

            if (!isNotebook && !isMoma && !isDataflow) {
                python("samuelPreDeploy")
            }

            adapter("deploy")
            python("release")
        }
    } finally {
        if (python.isBuildable(false)) {
            python("vTrack")
        }
        python("containerClean")
        cleanWs()
    }
}
