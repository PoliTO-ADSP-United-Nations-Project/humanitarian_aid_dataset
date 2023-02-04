# Time-Series Forecasting for Humanitarian Aid - ***VAL2G dataset***

We present in this repository the most important result of the project carried out in collaboration with the UNSSC: a new dataset for time-series forecasting on migratory flows.

To the best of our knowledge, no similar dataset currently exists that aggregates the information we report, or that could be used for a similar purpose.

## Dataset Description
The dataset created starts from a preliminary assumption: in general, the first countries of arrival in Europe are Italy, Spain, and Greece. This assumption derives not only from the evident geographical conformations of the Mediterranean and the migratory routes, but also from the scarcity of data regarding the rest of the European nations.

Given this assumption, we show in this graph the countries of origin of the potential migratory flows, which have a more consistent impact on the data.
<div align="center">
  <img src="https://github.com/PoliTO-ADSP-United-Nations-Project/.github/blob/main/imgs/country.png" title="countries" alt="countries" height="300"/>
</div>

You can directly download the final version of **VAL2G** dataset from Figshare [`link`](
https://figshare.com/articles/dataset/VAL2G_-_Dataset/22006961):
<div align="center">
  <img src="https://github.com/PoliTO-ADSP-United-Nations-Project/.github/blob/main/imgs/figshare.png" title="countries" alt="countries" height="50"/>
</div>

## **Features Description**

All the data are monthly based: this is the finest granularity that we found online and in addition, for the context in which this project is placed, it would make no sense to guarantee a daily prediction as it would not be usable.
The final dataset is composed of the following features:

|**Feature**         |**Type**         |**Description**        | **Source ID** |
|:-------------:|:---------------:|:----------:|:-------:|
|Monthly inflow| `Integer`| Migrant from a departure country to the arrival country.|`A`|
| Fatalities | `Integer` | Number of death in the country of departure.|`C`|
|HDI|`Float`| Human development index (i.e. statistic composite index of life expectancy, education, and per capita income indicators).|`B`|
|Distance Departure Destination| `Float` | Distance between capitals of departure and destination country in Km.|`D`|
|Percentage of currency change | `Float` | Currency change rate with respect to the previous month.|`E`|
|Sum Inflow| `Integer` | Total migrants' inflow per country of arrival.|`A`|
|Date | `String` | Month and year.|`Trasversal|
|Destination country|`String`|Destination country's ISO code.|`A`|
|Departure country|`String` | Departure country's ISO code.|`A`|
 

The dataset was built merging information from different resources, which include:
<div align="center">

|**ID**|**Type**         |**Source**         |🔗        |
|:------:|:-------------:|:---------------:|:----------:|
|`A`|***Migration Inflows*** | International Organization for Migration| [`Link`](https://migration.iom.int/europe/arrivals#content-tab-anchor)|
|`B`|***Social Indices*** | UN Development Reports| [`Link`](https://hdr.undp.org/data-center/documentation-and-downloads)|
|`C`|***Humanitarian Crisis*** | The ACLED| [`Link`](https://acleddata.com/data-export-tool/)|
|`D`|***Geographical References*** | The ACLED| [`Link`](https://acleddata.com/data-export-tool/)|
|`E`|***Currency Change*** | The ACLED| [`Link`](https://acleddata.com/data-export-tool/)|
</div>

-------------------------------------------------------------

## Cloning the repo
To cloning the repo throgh HTTPS or SSH, you must have installed Git on your operating system.<br>
Then you can open a new terminal and type the following command (this is the cloning throgh HTTPS):
```bash
    git clone https://github.com/PoliTO-ADSP-United-Nations-Project/humanitarian_aid_dataset
```
If you don't have installet Git, you can simply download the repository by pressing <i>"Download ZIP"</i>.

-------------------------------------------------------------

## Environment
Once the repo is cloned, some python libraries are required to properly set up your (virtual) environment.
They can be installed via pip:
```bash
    pip install -r requirements.txt
```
or via conda:
```bash
    conda create --name <env_name> --file requirements.txt
```

-------------------------------------------------------------

## Execution
The `main.py` is the entry point of the execution.<br>
You can run the program in this way:
```bash
python main.py
```
The program was built with a python version ```>= 3.8```: any lower version will not guarantee the correct execution of the software.
