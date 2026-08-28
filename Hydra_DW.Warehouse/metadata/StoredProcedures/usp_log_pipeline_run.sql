CREATE   PROCEDURE metadata.usp_log_pipeline_run
    @control_id     bigint,
    @run_id         varchar(100),
    @parent_run_id  varchar(100)  = NULL,
    @pipeline_name  varchar(200)  = NULL,
    @activity_name  varchar(100)  = NULL,
    @start_time     datetime2(6),
    @end_time       datetime2(6)  = NULL,
    @status         varchar(20),
    @row_count      bigint        = NULL,
    @error_message  varchar(4000) = NULL,
    @new_watermark  varchar(50)   = NULL
AS
BEGIN
    INSERT INTO metadata.pipeline_control_log (
        control_id, run_id, parent_run_id, pipeline_name, activity_name,
        start_time, end_time, duration_seconds, status, row_count,
        error_message, new_watermark_value, created_on
    )
    VALUES (
        @control_id, @run_id, @parent_run_id, @pipeline_name, @activity_name,
        @start_time, @end_time,
        DATEDIFF(SECOND, @start_time, ISNULL(@end_time, @start_time)),
        @status, @row_count, LEFT(@error_message, 4000),
        @new_watermark, SYSUTCDATETIME()
    );
END