import { Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { QualityAssessmentEntity } from 'src/entities/quality-assessment.entity';
import { ILoggerRepository } from 'src/interfaces/logger.interface';
import { IQualityAssessmentRepository } from 'src/interfaces/quality-assessment.interface';
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
}
