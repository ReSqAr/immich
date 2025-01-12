import {MigrationInterface, QueryRunner} from "typeorm";
import {
    migration1735468387695CreateAssetAnalysis,
    migration1735468387695CreateAssetDbscan,
    migration1735468387695CreateAssetDbscanClusters,
    migration1735468387695CreateAssetHomeDetection,
    migration1735468387695CreateAssetPhotoClassification
} from "src/resources/sql";

export class MemorylaneMaterialisedViews1735468387695 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(migration1735468387695CreateAssetHomeDetection);
        await queryRunner.query(migration1735468387695CreateAssetPhotoClassification);
        await queryRunner.query(migration1735468387695CreateAssetDbscan);
        await queryRunner.query(migration1735468387695CreateAssetDbscanClusters);
        await queryRunner.query(migration1735468387695CreateAssetAnalysis);
        await queryRunner.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_dbscan_id_pkey ON asset_dbscan (id);`);
        await queryRunner.query(`CREATE INDEX IF NOT EXISTS idx_asset_dbscan_owner_cluster ON asset_dbscan ("ownerId", cluster_id);`);
        await queryRunner.query(`CREATE INDEX IF NOT EXISTS idx_asset_dbscan_clusters_owner_cardinality ON asset_dbscan_clusters ("ownerId", cluster_cardinality_score_ge_0);`);
        await queryRunner.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_analysis_id_pkey ON asset_analysis (id);`);
        await queryRunner.query(`CREATE INDEX IF NOT EXISTS idx_asset_analysis_owner_timeline ON asset_analysis ("ownerId", ts);`);
        await queryRunner.query(`CREATE INDEX IF NOT EXISTS idx_asset_analysis_owner_cluster_lookup ON asset_analysis ("ownerId", cluster_id);`);
        await queryRunner.query(`CREATE INDEX IF NOT EXISTS idx_asset_analysis_quality_score_coalesce ON asset_analysis ((COALESCE(normalized_quality_score, 0)));`);
        await queryRunner.query(`CREATE INDEX IF NOT EXISTS idx_asset_analysis_quality_owner_timeline ON asset_analysis (COALESCE(normalized_quality_score, 0), "ownerId", ts);`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query("DROP INDEX IF EXISTS idx_asset_dbscan_id_pkey;");
        await queryRunner.query("DROP INDEX IF EXISTS idx_asset_dbscan_owner_cluster;");
        await queryRunner.query("DROP INDEX IF EXISTS idx_asset_dbscan_clusters_owner_cardinality;");
        await queryRunner.query("DROP INDEX IF EXISTS idx_asset_analysis_id_pkey;");
        await queryRunner.query("DROP INDEX IF EXISTS idx_asset_analysis_owner_timeline;");
        await queryRunner.query("DROP INDEX IF EXISTS idx_asset_analysis_owner_cluster_lookup;");
        await queryRunner.query("DROP INDEX IF EXISTS idx_asset_analysis_quality_score_coalesce;");
        await queryRunner.query("DROP INDEX IF EXISTS idx_asset_analysis_quality_owner_timeline;");
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_analysis;");
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_dbscan_clusters;");
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_dbscan;");
        await queryRunner.query("DROP VIEW IF EXISTS asset_photo_classification;");
    }
}
