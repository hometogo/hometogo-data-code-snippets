# Overview

This directory contains a connector that aims to keep dbt and Superset in sync. Further info can be found in the [this article](https://).

## How does it work?

Every time that the python script runs, it compares the physical tables imported on Superset to the tables coming out of the dbt project.

If a table is added to dbt, and is not yet imported on Superset, it imports the table to Superset through an API call.

If a table is imported on Superset, and it does not exist on dbt, it deletes the reference to this table on Superset through an API call.

## How can I benefit out of it?

By running this connector daily, you will ensure that the lineage between dbt and Superset is clear, and that all the changes on your dbt model will be reflected on Superset too.

### Further notes

This script was built inspired on [this package from Slidoapp](https://github.com/slidoapp/dbt-superset-lineage).  
