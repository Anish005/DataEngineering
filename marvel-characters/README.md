<h1 align="center">
Marvelous MLOps Free End-to-end MLOps with Databricks Course

## Set up your environment
In this course, we use Databricks serverless [version 3](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/three)

In our examples, we use UV. Check out the documentation on how to install it: https://docs.astral.sh/uv/getting-started/installation/

To create a new environment and create a lockfile, run:

```
uv sync --extra dev
```



# Data
Using the [**Marvel Characters Dataset**](https://www.kaggle.com/datasets/mohitbansal31s/marvel-characters?resource=download) from Kaggle.

This dataset contains detailed information about Marvel characters (e.g., name, powers, physical attributes, alignment, etc.).
It is used to build classification and feature engineering models for various MLOps tasks, such as predicting character attributes or status.

# Scripts

- `01.process_data.py`: Loads and preprocesses the Marvel dataset, splits into train/test, and saves to the catalog.
- `02.train_register_fe_model.py`: Performs feature engineering and trains the Marvel character model.
- `03.deploy_model.py`: Deploys the trained Marvel model to a Databricks model serving endpoint.
- `04.post_commit_status.py`: Posts status updates for Marvel integration tests to GitHub.
- `05.refresh_monitor.py`: Refreshes monitoring tables and dashboards for Marvel model serving.

# MLOps with Databricks: Marvel Characters Project

## Project Overview
This project serves as a complete end-to-end MLOps pipeline designed to complement the "MLOps with Databricks: Free Edition" hands-on course [1, 2]. It uses a **Marvel Characters Dataset from Kaggle** to build classification and feature engineering models, leveraging **Databricks serverless version 3** and **UV** for environment and dependency management [1].

## Theoretical Concepts & Practical Implementations

### 1. Development & Data Processing
*   **Theoretical Concept:** Establishing a solid foundation for MLOps and developing robust machine learning environments on Databricks [2].
*   **Project Implementation:** The data pipeline begins with the `01.process_data.py` script, which loads the Kaggle dataset, preprocesses the data, splits it into training and testing sets, and saves the final output to the catalog [3]. 

### 2. MLflow & Experiment Tracking
*   **Theoretical Concept:** Utilizing **MLflow** to manage the machine learning lifecycle by logging metrics, parameters, and artifacts, as well as securely registering the models [2].
*   **Project Implementation:** The `02.train_register_fe_model.py` script is used to perform feature engineering, train the Marvel character model, and systematically register it [3].

### 3. Model Serving & Deployment
*   **Theoretical Concept:** Understanding various model serving architectures and learning how to deploy models as scalable endpoints for real-time inference [2].
*   **Project Implementation:** The `03.deploy_model.py` script automates the process of deploying the trained model directly to a **Databricks model serving endpoint** [3].

### 4. Infrastructure, CI/CD & Automation
*   **Theoretical Concept:** Automating deployments and managing infrastructure as code using **Databricks Asset Bundles** and **Continuous Integration/Continuous Deployment (CI/CD)** strategies [2].
*   **Project Implementation:** Infrastructure is managed via the `databricks.yml` file, while CI/CD automation is handled using GitHub Actions located in the `.github/workflows` directory [4]. Furthermore, the `04.post_commit_status.py` script runs to post integration test status updates back to GitHub [3].

### 5. Lakehouse Monitoring
*   **Theoretical Concept:** Maintaining model performance in production by implementing data and model monitoring within the Databricks Lakehouse architecture [2].
*   **Project Implementation:** The lifecycle concludes with the `05.refresh_monitor.py` script, which is responsible for refreshing monitoring tables and dashboards to continuously track the health of the served Marvel model [3].