CREATE TABLE [metadata].[pipeline_control_log] (

	[log_id] bigint IDENTITY NOT NULL, 
	[control_id] bigint NOT NULL, 
	[run_id] varchar(100) NOT NULL, 
	[parent_run_id] varchar(100) NULL, 
	[pipeline_name] varchar(200) NULL, 
	[activity_name] varchar(100) NULL, 
	[start_time] datetime2(6) NOT NULL, 
	[end_time] datetime2(6) NULL, 
	[duration_seconds] bigint NULL, 
	[status] varchar(20) NOT NULL, 
	[row_count] bigint NULL, 
	[error_message] varchar(4000) NULL, 
	[new_watermark_value] varchar(50) NULL, 
	[created_on] datetime2(6) NOT NULL
);