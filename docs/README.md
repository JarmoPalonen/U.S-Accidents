# Factors Behind Severe Road Accidents in the U.S
Team name: Syntax Terror

Members:
- Emmi Halonen
- Emma Lehtoranta
- Jarmo Palonen
- Tiina Serkosalmi
- Heikki Virtanen

## Setup & Installation
### Prerequisites:

#### Java Development Kit (JDK)
  - Version: JDK 17
  - Download link: https://adoptium.net/en-GB/temurin/releases?package=jdk&version=17&os=windows&arch=x64
    
#### Apache Spark
  - Version 3.5.x
  - Download link: https://spark.apache.org/downloads.html
  - Extract to for example C:\spark

#### Hadoop Winutils
  - Download link: https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.6/bin/winutils.exe
  - Create a directory C:\hadoop\bin and place the .exe inside
    
#### Python 3.11
#### Libraries
   - pyspark
   - pandas
   - pymongo
   - matplotlib
   - seaborn

###### Use for example Conda Environment and run
    pip install pyspark pandas pymongo matplotlib seaborn



#### Configuring Environment Variables
Go to System Properties -> Advanced -> Environment variables and set:

| Variable Name | Variable Value |
| -------- | ------- |
| PYSPARK_PYTHON  | Path to your Python executable (3.11)    |
| JAVA_HOME | The folder where you extracted Java Development Kit to     |
| PYSPARK_PYTHON    | Path to your Python executable (3.11)    |
  
##### MongoDB
- Download link: https://www.mongodb.com/try/download/community-kubernetes-operator

#### Dataset
- Download the U.S. Accidents (March 23) dataset
- Download link: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents
  - Place the US_Accidents_March23.csv file inside the /data folder.

#### How to Run the Project
- Step 1: Activate your Conda Environment
- Step 2: Run git clone https://github.com/JarmoPalonen/U.S-Accidents or just download the files
- Step 3: Open the folder in your IDE and run the Final_Code.ipynb from the main folder

p
