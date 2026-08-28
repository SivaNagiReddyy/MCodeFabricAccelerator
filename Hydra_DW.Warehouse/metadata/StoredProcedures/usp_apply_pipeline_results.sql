CREATE   PROCEDURE metadata.usp_apply_pipeline_results
    @parent_run_id varchar(100)
AS
BEGIN
    UPDATE m
    SET m.last_load_status     = l.status,
        m.last_load_start_time = l.start_time,
        m.last_load_end_time   = l.end_time,
        m.last_row_count       = l.row_count,
        m.last_error_message   = l.error_message,
        m.last_run_id          = l.run_id,
        m.last_parent_run_id   = l.parent_run_id,
        m.updated_on           = SYSUTCDATETIME(),
        m.watermark_value = CASE
            WHEN l.status = 'Succeeded'
             AND m.load_type = 'Incremental'
             AND NULLIF(l.new_watermark_value, '') IS NOT NULL
            THEN l.new_watermark_value
            ELSE m.watermark_value
        END
    FROM metadata.pipeline_control m
    JOIN metadata.pipeline_control_log l
      ON l.control_id = m.control_id
    WHERE l.parent_run_id = @parent_run_id
      AND l.log_id = (
          SELECT TOP 1 l2.log_id
          FROM metadata.pipeline_control_log l2
          WHERE l2.control_id    = l.control_id
            AND l2.parent_run_id = @parent_run_id
          ORDER BY l2.created_on DESC
      );
END