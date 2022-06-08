# Overview

This directory contains: a connector that aims to keep dbt and Superset in sync, and a script that pushes dbt metadata onto Superset. Further info can be found in [this article](https://).

## How does the connector work?

Every time that it runs, it compares the physical tables imported on Superset to the tables coming out of the dbt project.

If a table is added to dbt, and is not yet imported on Superset, it imports the table to Superset through an API call.

If a table is imported on Superset, and it does not exist on dbt, it deletes the reference to this table on Superset through an API call.

## How does the metadata script work?

Every time that it runs, it pushes all the physical tables' column descriptions available on dbt to Superset.

If there is no description for a given column, the column remains untouched.

## How can I benefit out of it?

By running the connector daily, you will ensure that the lineage between dbt and Superset is clear, and that all the changes on your dbt model will be reflected on Superset too.

By running the metadata script regularly, you will make metadata more accessible to Superset users, consequently easing their way around data.

### Further notes

The code was built inspired on [this package from Slidoapp](https://github.com/slidoapp/dbt-superset-lineage).  
