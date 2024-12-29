import { Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { QualityAssessmentEntity } from 'src/entities/quality-assessment.entity';
import { ILoggerRepository } from 'src/interfaces/logger.interface';
import { Distribution, IQualityAssessmentRepository } from 'src/interfaces/quality-assessment.interface';
import { Repository } from 'typeorm';

@Injectable()
export class QualityAssessmentRepository implements IQualityAssessmentRepository {
  constructor(
    @Inject(ILoggerRepository) protected logger: ILoggerRepository,
    @InjectRepository(QualityAssessmentEntity) private qualityAssessmentRepository: Repository<QualityAssessmentEntity>,
  ) {}

  async upsert(assetId: string, score: number): Promise<void> {
    await this.qualityAssessmentRepository.upsert({ assetId, score }, { conflictPaths: ['assetId'] });
  }

  async clearAllIQAScores(): Promise<void> {
    return this.qualityAssessmentRepository.clear();
  }

  async scoreDistribution(userIds: string[]): Promise<Distribution> {
    const result = await this.qualityAssessmentRepository.query(scoreDistributionQuery, [userIds]);
    if (result && result.length > 0) {
      return result[0] as Distribution;
    }
    return { mean: 0, stddev: 0 };
  }
}

const scoreDistributionQuery = `
    SELECT AVG(qa.score) as mean,
           STDDEV(qa.score) as stddev
    FROM public.assets a
             JOIN public.quality_assessments qa on a.id = qa."assetId"
    WHERE a."ownerId" = ANY($1::uuid[])
`;
