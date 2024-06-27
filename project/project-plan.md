# Project Plan

## Project Title
Impact of Climate Change on Health Outcomes

## Summary

This data engineering project aims to analyze the **Impact of Climate Change on Health Outcomes** by examining global temperature data and health data related to mortality rates due to non-communicable diseases. The project will use two data sources: [Berkley Earth Surface Temperature Data](https://www.kaggle.com/datasets/josepferrersnchez/bearkley-earth-surface-temperature-data) and [WHO Mortality Data](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/probability-(-)-of-dying-between-age-30-and-exact-age-70-from-any-of-cardiovascular-disease-cancer-diabetes-or-chronic-respiratory-disease). The analysis will focus on identifying patterns and trends in temperature changes from 2000 to 2019 and correlating these changes with health outcomes globally.

## Rationale

The analysis of global temperature changes and health outcomes can have several significant impacts, including:
1. **Policy Makers:** The analysis can help policy makers to develop informed policies aimed at mitigating the adverse health effects of climate change.
2. **Healthcare Providers:** The findings can guide healthcare providers to prepare for and manage the health impacts of climate change.
3. **Researchers:** It provides a basis for further research into the long-term health effects of climate change.

Overall, the analysis aims to provide valuable insights into the relationship between climate change and health outcomes, benefiting policy makers, healthcare providers, and researchers.

## Datasources

### Datasource1: Global Temperature Data
* Metadata URL: [https://www.kaggle.com/datasets/josepferrersnchez/bearkley-earth-surface-temperature-data](https://www.kaggle.com/datasets/josepferrersnchez/bearkley-earth-surface-temperature-data)
* Sample Data URL: [https://www.kaggle.com/datasets/josepferrersnchez/bearkley-earth-surface-temperature-data](https://www.kaggle.com/datasets/josepferrersnchez/bearkley-earth-surface-temperature-data)
* Data Type: CSV

This data source contains global surface temperature data provided by Berkley Earth. The data includes monthly average temperatures and temperature anomalies for various countries from 2000 to 2019.

### Datasource2: Global Health Data
* Metadata URL: [https://www.who.int/data/gho/data/indicators/indicator-details/GHO/probability-(-)-of-dying-between-age-30-and-exact-age-70-from-any-of-cardiovascular-disease-cancer-diabetes-or-chronic-respiratory-disease](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/probability-(-)-of-dying-between-age-30-and-exact-age-70-from-any-of-cardiovascular-disease-cancer-diabetes-or-chronic-respiratory-disease)
* Sample Data URL: [https://www.who.int/data/gho/data/indicators/indicator-details/GHO/probability-(-)-of-dying-between-age-30-and-exact-age-70-from-any-of-cardiovascular-disease-cancer-diabetes-or-chronic-respiratory-disease](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/probability-(-)-of-dying-between-age-30-and-exact-age-70-from-any-of-cardiovascular-disease-cancer-diabetes-or-chronic-respiratory-disease)
* Data Type: CSV

This data source contains health data from the World Health Organization (WHO), detailing the probability of dying between exact ages 30 and 70 from non-communicable diseases for various countries from 2000 to 2019.

## Work Packages

1. Extract Data from Multiple Sources [#1][i1]
2. Implement Data Transformation Step in ETL Pipeline [#2][i2]
3. Implement Data Loading Step in ETL Data Pipeline [#3][i3]
4. Automated Tests for the Project [#4][i4]
5. Continuous Integration Pipeline for the Project [#5][i5]
6. Final Report and Presentation Submission [#6][i6]

[i1]: https://github.com/YourUsername/YourRepo/issues/1
[i2]: https://github.com/YourUsername/YourRepo/issues/2
[i3]: https://github.com/YourUsername/YourRepo/issues/3
[i4]: https://github.com/YourUsername/YourRepo/issues/4
[i5]: https://github.com/YourUsername/YourRepo/issues/5
[i6]: https://github.com/YourUsername/YourRepo/issues/6
