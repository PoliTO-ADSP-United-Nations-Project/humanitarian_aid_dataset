# Time-Series Forecasting for Humanitarian Aid - ***dataset***

We present in this repository the most important result of the project carried out in collaboration with the UNSSC: a new dataset for time-series forecasting on migratory flows.

To the best of our knowledge, no similar dataset currently exists that aggregates the information we report, or that could be used for a similar purpose.

## Dataset Description
The dataset created starts from a preliminary assumption: in general, the first countries of arrival in Europe are Italy, Spain, and Greece. This assumption derives not only from the evident geographical conformations of the Mediterranean and the migratory routes, but also from the scarcity of data regarding the rest of the European nations.

Given this assumption, we show in this graph the countries of origin of the potential migratory flows, which have a more consistent impact on the data.
<div align="center">
  <img src="https://github.com/PoliTO-ADSP-United-Nations-Project/.github/blob/main/imgs/country.png" title="countries" alt="countries" height="300"/>
</div>

The dataset was built merging information from different resources, which include:
<div align="center">

|**Type**         |**Source**         |🔗        |
|:-------------:|:---------------:|:----------:|
|***Climate Events*** | The World Bank CCKP| [`Link`](https://climateknowledgeportal.worldbank.org/)|
|***Migration Inflows*** | International Organization for Migration| [`Link`](https://migration.iom.int/europe/arrivals#content-tab-anchor)|
|***Social Indices*** | UN Development Reports| [`Link`](https://hdr.undp.org/data-center/documentation-and-downloads)|
|***Humanitarian Crisis*** | The ACLED| [`Link`](https://acleddata.com/data-export-tool/)|

</div>

-------------------------------------------------------------

## Cloning the repo
To cloning the repo throgh HTTPS or SSH, you must have installed Git on your operating system.<br>
Then you can open a new terminal and type the following command (this is the cloning throgh HTTPS):
```bash
    git clone https://github.com/fracapuano/NetworkDynamics.git
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