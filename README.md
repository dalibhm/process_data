The poroject preprocesses data:
1/ a quarterly file of corporate earnings
2/ a daily file of stock quotes

The reconciliation of fundamental data and historical data produces the input to a machine learning software.

I use Spark to process large files and merge them in a way that is several orders of magnitude longer with Python dataframes.
