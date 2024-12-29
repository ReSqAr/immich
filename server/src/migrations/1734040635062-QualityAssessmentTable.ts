import { MigrationInterface, QueryRunner } from "typeorm";

export class QualityAssessmentTable1734040635062 implements MigrationInterface {
    name = 'QualityAssessmentTable1734040635062'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "quality_assessment" ("assetId" uuid NOT NULL, "score" double precision NOT NULL, CONSTRAINT "PK_7cfc1a98a265660a4240b25a4a8" PRIMARY KEY ("assetId"))`);
        await queryRunner.query(`ALTER TABLE "quality_assessment" ADD CONSTRAINT "FK_7cfc1a98a265660a4240b25a4a8" FOREIGN KEY ("assetId") REFERENCES "assets"("id") ON DELETE CASCADE ON UPDATE NO ACTION`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "quality_assessment" DROP CONSTRAINT "FK_7cfc1a98a265660a4240b25a4a8"`);
        await queryRunner.query(`DROP TABLE "quality_assessment"`);
    }

}
