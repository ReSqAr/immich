import {MigrationInterface, QueryRunner} from "typeorm";
import {
    migrationCreateAssetAnalysis,
    migrationCreateAssetDbscan,
    migrationCreateAssetDbscanClusters,
    migrationCreateAssetHomeDetection, migrationCreateAssetPhotoClassification
} from "src/resources/sql";

export class MemorylaneMaterialisedViews1735468387695 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(migrationCreateAssetHomeDetection);
        await queryRunner.query(migrationCreateAssetPhotoClassification);
        await queryRunner.query(migrationCreateAssetDbscan);
        await queryRunner.query(migrationCreateAssetDbscanClusters);
        await queryRunner.query(migrationCreateAssetAnalysis);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_analysis;");
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_dbscan_clusters;");
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_dbscan;");
        await queryRunner.query("DROP VIEW IF EXISTS asset_photo_classification;");
        await queryRunner.query("DROP VIEW IF EXISTS asset_home_detection;");
    }
}
