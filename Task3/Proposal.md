# Problem statment
Uses Machine Learning techniques to manage and optimize spending on promotion to increase sales, brand awareness etc. 

# Data Overview

Historical sales data from 2013 to 2015 from 1,115 stores.

## Available features 
-   **Id** - an Id that represents a (Store, Date) duple within the test set
-   **Store** - a unique Id for each store
-   **Sales** - the turnover for any given day (this is what you are predicting)
-   **Customers** - the number of customers on a given day
-   **Open** - an indicator for whether the store was open: 0 = closed, 1 = open
-   **StateHoliday** - indicates a state holiday. Normally all stores, with few exceptions, are closed on state holidays. Note that all schools are closed on public holidays and weekends. a = public holiday, b = Easter holiday, c = Christmas, 0 = None
-   **SchoolHoliday** - indicates if the (Store, Date) was affected by the closure of public schools
-   **StoreType** - differentiates between 4 different store models: a, b, c, d
-   **Assortment** - describes an assortment level: a = basic, b = extra, c = extended
-   **CompetitionDistance** - distance in meters to the nearest competitor store
-   **CompetitionOpenSince[Month/Year]** - gives the approximate year and month of the time the nearest competitor was opened
-   **Promo** - indicates whether a store is running a promo on that day
-   **Promo2** - Promo2 is a continuing and consecutive promotion for some stores: 0 = store is not participating, 1 = store is participating
-   **Promo2Since[Year/Week]** - describes the year and calendar week when the store started participating in Promo2
-   **PromoInterval** - describes the consecutive intervals Promo2 is started, naming the months the promotion is started anew. E.g. "Feb,May,Aug,Nov" means each round starts in February, May, August, November of any given year for that store

# Model Proposal and TPM/TPO
For this use case I would start by performing EDA and asees preliminary feature importances with Boruta.
Then I would use a forecasting model such as Prophet, LSTM's or XGBoost to train with the historical data and construct a set of models that performs well for all stores or a subset of them. With this models I would extract feature importances to extract the most relevant features of each model, Then based on this information we could better manage and optimize promotion efforts.
Some times this type of results are difficult to interpret for non technical colleges. So make things more interpretable I would also combine some metrics into a new target variable that measures the effectiveness of a promotion, for example Sales before and after a promotion, In this way creating simple binary classification problem. To this new target variable I would apply a highly interpretable model such as [FIGS](https://arxiv.org/abs/2201.11931) such that the business colleagues would be able to follow the tree structure and get an intuitive understanding of how the relevant features relate to each-other a successful promotion.