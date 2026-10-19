"""Shared migration scenarios for isolated local MySQL and dedicated VM test schemas."""
import json
from uuid import UUID, uuid4
from product_pipeline.contracts import canonical
from product_pipeline.files import atomic_json
from product_pipeline.publication.mysql import MySQL
from product_pipeline.publication.runner import publish
from product_pipeline.publication.schema import reference_ddl
from publication_fixture import fixture


def run_cases(execute, sql_file, connect, output, report, migration, base_ddl,
              schema_prefix='pipeline_migration_'):
    apply = (migration / 'feature25_apply.sql').read_text(encoding='utf-8-sig')
    rollback = (migration / 'feature25_rollback.sql').read_text(encoding='utf-8-sig')

    def database(name, ddl=None):
        name = schema_prefix + name
        execute('CREATE DATABASE `' + name + '` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        if ddl:
            sql_file(name, ddl)
        return name

    def shape(name):
        tables = execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + name
                         + "' AND TABLE_NAME<>'_feature25_schema_history' ORDER BY TABLE_NAME")
        return {r['TABLE_NAME']: execute(f'SHOW CREATE TABLE `{name}`.`{r["TABLE_NAME"]}`')[0]['Create Table']
                for r in tables}

    def checked(name):
        report['checks'].append(name)
        atomic_json(output / 'report.json', report)
        print(json.dumps({'check': name, 'status': 'PASS'}), flush=True)

    a = database('empty', base_ddl())
    before = shape(a)
    assert 'MYSQL_SCHEMA_MATCHED_DATA_NOT_PUBLISHED' in sql_file(a, apply)
    connection = connect(a)
    db = MySQL(dict(contract_version=1, database=a, target_id='isolated-migration'), connection=connection)
    report['preflight'] = db.preflight()
    db.close()
    checked('empty_v1_apply_matches_v2_runtime_preflight')
    after = shape(a)
    sql_file(a, apply)
    assert shape(a) == after
    checked('apply_is_idempotent')
    sql_file(a, rollback)
    assert shape(a) == before
    checked('rollback_restores_original_tables_columns_indexes_and_fks')
    assert 'ALREADY_ROLLED_BACK' in sql_file(a, rollback)
    checked('rollback_is_idempotent')
    sql_file(a, apply)
    assert shape(a) == after
    checked('reapply_after_rollback')

    b = database('partial', base_ddl())
    before = shape(b)
    point = 'ALTER TABLE `products` ADD COLUMN `attributes_json` JSON NOT NULL;'
    assert apply.count(point) == 1
    injected = apply.replace(point, "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='TEST_INJECTED_DDL_FAILURE';")
    sql_file(b, injected, expected='TEST_INJECTED_DDL_FAILURE')
    assert 'manufacturer' in shape(b)['products']
    checked('mid_ddl_failure_keeps_recovery_journal')
    sql_file(b, apply)
    checked('original_apply_resumes_after_partial_ddl')
    sql_file(b, rollback)
    assert shape(b) == before
    checked('rollback_after_partial_apply_preserves_baseline')

    c = database('backfill', base_ddl())
    sql_file(c, "INSERT INTO categories VALUES ('" + '1' * 32 + "','fixture','fixture',1,1);"
             "INSERT INTO products (id,product_key,name,category_id,source_code,source_product_id,status,dedup_status,created_at,updated_at) "
             "VALUES ('" + '2' * 32 + "','danawa:fixture','fixture','" + '1' * 32
             + "','danawa','fixture','ACTIVE','CONFIRMED',UTC_TIMESTAMP(3),UTC_TIMESTAMP(3));")
    before = shape(c)
    sql_file(c, apply, expected='BACKFILL_REQUIRED_PRODUCTS_NO_TABLE_DDL_APPLIED')
    assert shape(c) == before
    checked('existing_v1_rows_block_without_fake_backfill')

    d = database('existing', base_ddl())
    sql_file(d, 'ALTER TABLE products ADD COLUMN manufacturer VARCHAR(255) NULL;')
    before = shape(d)
    sql_file(d, apply)
    sql_file(d, rollback)
    assert shape(d) == before
    checked('preexisting_extension_is_preserved')

    e = database('wrong', base_ddl())
    sql_file(e, 'ALTER TABLE products ADD COLUMN manufacturer VARCHAR(254) NULL;')
    before = shape(e)
    sql_file(e, apply, expected='EXISTING_COLUMN_DIFFERS_NO_TABLE_DDL_APPLIED')
    assert shape(e) == before
    checked('different_existing_column_blocks_changes')

    f = database('missing')
    sql_file(f, apply, expected='BASE_10_INNODB_TABLES_REQUIRED')
    assert not shape(f)
    checked('missing_base_schema_is_not_bootstrapped')

    g = database('extra', base_ddl())
    sql_file(g, 'ALTER TABLE products ADD COLUMN service_owned_note VARCHAR(10) NOT NULL;')
    before = shape(g)
    sql_file(g, apply, expected='UNMAPPED_REQUIRED_COLUMN_NO_TABLE_DDL_APPLIED')
    assert shape(g) == before
    checked('unmapped_required_column_blocks_changes')

    h = database('already_v2', reference_ddl())
    before = shape(h)
    sql_file(h, apply)
    sql_file(h, rollback)
    assert shape(h) == before
    checked('already_v2_schema_is_preserved_on_rollback')

    connection = connect(a)
    db = MySQL(dict(contract_version=1, database=a, target_id='isolated-migration'), connection=connection)
    plan, guard = fixture(output / 'publication-fixture', db.target)
    catalog = plan['products'][0]['group']['tables']['product_catalog'][0]
    with db.transaction():
        db.insert('categories', dict(id=UUID(catalog['service_category_id']).hex, code=catalog['service_category_code'],
            name='fixture', active=True, recognition_enabled=True))
        db.insert('pipeline_publication_guards', dict(id=uuid4().hex, product_key=catalog['product_key'],
            revision=1, guard_json=canonical(guard)))
    assert publish(plan, db)['status'] == 'COMPLETED'
    db.close()
    before = shape(a)
    sql_file(a, rollback, expected='ROLLBACK_REFUSED_')
    assert shape(a) == before
    checked('published_data_prevents_destructive_rollback')

    i = database('drift', base_ddl())
    sql_file(i, apply)
    sql_file(i, 'ALTER TABLE products ADD COLUMN external_probe VARCHAR(10) NULL;')
    before = shape(i)
    sql_file(i, rollback, expected='SCHEMA_CHANGED_AFTER_RECORDED_STEP_REVIEW_REQUIRED')
    assert shape(i) == before
    checked('external_schema_change_blocks_rollback')

    report.update(passed=True, check_count=len(report['checks']), server_version=execute('SELECT @@version AS v')[0]['v'])
    atomic_json(output / 'report.json', report)
    print(json.dumps(report), flush=True)
