CREATE TABLE [dbo].[gold_fact_transactions] (

	[txn_id] varchar(20) NULL, 
	[store_id] varchar(10) NULL, 
	[customer_id] varchar(20) NULL, 
	[txn_date] date NULL, 
	[product_id] varchar(20) NULL, 
	[quantity] int NULL, 
	[unit_price] float NULL, 
	[total_amount] float NULL, 
	[last_updated] datetime2(0) NULL
);