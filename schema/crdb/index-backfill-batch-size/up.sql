-- NON-TRANSACTIONAL
--
-- `SET CLUSTER SETTING` cannot be used inside a transaction.
SET CLUSTER SETTING bulkio.index_backfill.batch_size = 5000;
