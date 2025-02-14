### Jacob Adler Brightwheel Assignment

This repo uses dbt and duckdb to combine the four provided prospective leads datasets. The goal was a model with one row per lead that can answer business questions.

To simulate a production environment (especially one with 100 GB source data), I used duckdb's ability to write SQL statements on top of CSV files rather than use dbt's seed functionality. dbt seed is not recommended for production data or large datasets.

In each staging model, I attempt to do some basic data cleaning. With more time, this is an area I'd spend more time in. Cleaner data would improve our matching of leads between sources as well as make the data more helpful to the sales team.

The combined leads model is where I try to combine the data across sources. With limited time, my first draft includes only a join on the phone number, which the assignment suggested should be the primary identifier. With more time, I would also try to fuzzy match on address. I have a test asserting that the phone column is unique. This fails in a few places, I think because they are one person with multiple locations. I would probably add a unique key here on phone and address and assert uniqueness on that instead.

The business questions helped me think about how to reconcile the methodology for combining these different data sources. I decided between either joining + coalescing or unioning + window function, though I now suspect that the best implementation should invoke dbt snapshots.

- I chose to join the different sources on phone number. This gave me a lot of flexibility to then go and choose the priority to accept values from each source in coalesce statements. As I went further, I didn't feel great about how well this implementation would be for changing schemas or the questions tracking which leads came from which schema or changes since previous loads. 
- If we unioned the data, we could use window functions to go through each data source from highest quality to least and select the first non-null value. You could also use a qualify statement to select the entirety of one data source record and ignore any other duplicate data sources. For the latter, I think that will throw away some potentially enriching data that may not be included in the other source.
- A snapshot or change table may be helpful for this case, since the CSVs will have a lot of repeat records, and you get some built-in change tracking. dbt snapshots also do a good job of handling schema changes, and you can apply some logic in your dbt models based on the snapshot date if you need to use a different regex on a string column, for example. Of course, even with snapshots you still need a solution for consolidating the data down to one row per lead. Additionally, models building on top of dbt snapshots also tend to suit themselves well to incremental materializations, which is helpful for 100 GB source files. If I were to start from scratch with more time, and more data, I'd probably try this out.

Since the sample datasets were small, I thumbed through each in Google Sheets to get a feel for what to expect. I also ran SELECT statements in the duckdb CLI to confirm the dbt model outputs were as expected. However, I noticed that each source file came from a specific state (NV, OK, or TX), while the Salesforce data has dummy values for phone numbers. Both these made QA and testing my matching and deduplication efforts tough. With more time or data, I would be able to thoroughly test my code to assess accuracy and uncover edge cases.

As far as testing I'd implement with more time:
- Accepted values for categorical data, make sure hours and age ranges (after you clean the columns up) have reasonable values.
- Make sure phone numbers, emails, and addresses are in correct formats in staging tests. This increases the chances the leads will be able to be contacted, assuming the contact information is valid and accurate (something we'd run through a third-party vendor at a previous company).
- Data tests on top of the combined sources model to confirm one of the values for a business question (ex. how many from x source, or on x date).
- Data tests on top of a specific record present in multiple sources to confirm that the correct column values made their way into the final version, with correct source attribution.