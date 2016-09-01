# elastic-bus-data
This project contains resoures for putting data into Elasticsearch from a variety of sources, including
- Harvest (will work with both UK and US instances)
- USForex
- SalesForce
- Zendesk
- Expensify
- Quick Books Online
- 
## Harvest

## USForex
[This](http://www.usforex.com/forex-tools/historical-rate-tools/monthly-average-rates) is where we go to get foreign exchange rates.

Load the page that contains the prices that you want and copy them and paste into a spreadsheet. Format the date as Short Format Date in Excel. Save the data to a CSV file and name it according to the naming convention below (FROM-TO.csv). When you have updated all of the CSV files then run the usforex.rb program which will produce an elasticsearch bulk load file (it gets a .bulk extension). See the comments in usforex.rb for instructions on how to load the file from the command line. The mapping for the Elasticsearch index 'usforex' in in the file usforex.json. 
The process isn't fully automated and will likely want some hand-crafting along he way but it is small amounts of data so it probably isn't too onerous until we find a better way of doing it.

- GBP-USD - to go from GBP to USD then you must multiply by the rate in this file
- EUR-USD
- DKK-USD
- EUR-GBP
- DKK-GBP
