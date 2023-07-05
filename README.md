exampleenginepythonqiyhbwvw
===========================================


This scaffold contains a Engine example that works in both Local (PC) and Sandbox.

You can start developing your engine in 2 ways:
1. Adding a notebook inside the notebooks folder. You can use the examples that are in the `examples/` folder. You just need to copy the contents of the example folder you want to use into the root folder. For example, copy the contents of `examples/pyspark_contracts/` into the root folder and run notebooks `notebooks/engine.ipynb`.
```
cp -r examples/pyspark_contracts/* .
```
2. Generating your own python code inside the `exampleenginepythonqiyhbwvw/experiment.py` file.

How to use it in your Sandbox!
------------------------------

This repository has been created automatically by your Sandbox Manager. In this section it shows how to run it in your Sandbox creating a new Engine version.

The proposed basic workflow (just the first version) consists of the following stages:

1. In your Bitbucket repository, create a **Pull Request** to `master` branch from `develop`
2. Go to your Sandbox Manager and inside your Engine, click on **Create Version** button and it should automatically create a version 0.0.1
3. **Run this version** and check the execution logs

Congratulations you have your first Engine version created and executed!

If you want to modify the execution variables in the example, follow these steps:

1. Execution variables should be modified in the [application.conf ](resources/application.conf) configuration file. Note that the dictionary key is changed by `EnvironmentVarsPM` and all variables must be in capital letters. Next config file shows an example but add real hdfs paths from your sandbox

```
{
  EnvironmentVarsPM {
    INPUTCLIENTFILE = "/data/sandboxes/test/data/clients.csv"
    INPUTCLIENTFILE = ${?INPUTCLIENTFILE_OVERWRITE}
    INPUTCONTRACTFILE = "/data/sandboxes/test/data/contracts.csv"
    INPUTCONTRACTFILE = ${?INPUTCONTRACTFILE_OVERWRITE}
    INPUTPRODUCTFILE = "/data/sandboxes/test/data/products.csv"
    INPUTPRODUCTFILE = ${?INPUTPRODUCTFILE_OVERWRITE}
    OUTPUTFILE = "/data/sandboxes/test/data/output"
    OUTPUTFILE = ${?OUTPUTFILE_OVERWRITE}
  }
}
```

2. Include the application.conf path in the kaafile as:

```bash
[project]
type = "WORKER"
version = "0.1.0"
name = "%%%MAIN-MODULE-NAME%%%"
package = "pyspark"
config = "resources/application.conf"
```

3. As this Engine has a previous version that cannot be overwritten, it is necessary to change the version running `kaa version --set NEW_VERSION`, where NEW_VERSION could be increased as for example 0.0.2

4. In your Bitbucket repository, create a **Pull Request** to `master` branch from `develop`

5. Go to your Sandbox Manager and inside your Engine, click on **Create Version** button and it should automatically create a version as your set in `NEW_VERSION`

6. **Run this version** and check the execution logs


This example works also locally (in your PC) but you have to use the conf file of application_local.conf

How to use it locally!
----------------------

To be able to work in your own PC, it is necessary to have an IDE as a development environment as well as to have the environment prepared.

1. Clone the repository using SourceTree. SourceTree is a git client and you can find it in the software center

2. Create a virtual environment called venv with:
```bash
python3 -m venv venv
```

3. Activate the virtual environment with
```bash
venv\Scripts\deactivate
```

4. Kaa installation: For using Kaa locally, you can use the `install.py` script in `.kaa` directory found in the current project. In this step you will need your user and password from Artifactory
```bash
python .kaa/install.py
```

5. Install dependencies with: (In this step you will need your user and password from Artifactory)
```bash
kaa install
```

6. Check quality Assurance: Once you have kaa installed, you can launch all commands for executing tests and checking code against PEP8 guidelines:

* `kaa test` executes the tests using pytest package.
* `kaa lint` checks the source code to comply with PEP8 recommendations.

7. Run process locally: Once we have run all kaa commands, it is possible to launch locally the example.

> Check `jar_rel_path` path from the **venv** directory (ej. `.venv/share/sdk/jars/dataproc-sdk-all-0.3.9-3.1.jar`).

```bash
# Spark3.1 (Review java version, tested with openjdk 11.0.15)

spark-submit --jars <jar_rel_path> --master "local[*]" worker.py resources/application.conf
```

