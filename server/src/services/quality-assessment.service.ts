import { Injectable } from '@nestjs/common';
import { SystemConfig } from 'src/config';
import { OnEvent, OnJob } from 'src/decorators';
import { ImmichWorker } from 'src/enum';
import { WithoutProperty } from 'src/interfaces/asset.interface';
import { DatabaseLock } from 'src/interfaces/database.interface';
import { ArgOf } from 'src/interfaces/event.interface';
import { JOBS_ASSET_PAGINATION_SIZE, JobName, JobOf, JobStatus, QueueName } from 'src/interfaces/job.interface';
import { BaseService } from 'src/services/base.service';
import { getAssetFiles } from 'src/utils/asset.util';
import { isIQAModelKnown, isQualityAssessmentEnabled } from 'src/utils/misc';
import { usePagination } from 'src/utils/pagination';

@Injectable()
export class QualityAssessmentService extends BaseService {
  @OnEvent({ name: 'config.init' })
  async onConfigInit({ newConfig }: ArgOf<'config.init'>) {
    await this.init(newConfig);
  }

  @OnEvent({ name: 'config.update', server: true })
  async onConfigUpdate({ oldConfig, newConfig }: ArgOf<'config.update'>) {
    await this.init(newConfig, oldConfig);
  }

  @OnEvent({ name: 'config.validate' })
  onConfigValidate({ newConfig }: ArgOf<'config.validate'>) {
    if (!isIQAModelKnown(newConfig.machineLearning.iqa.modelName)) {
      throw new Error(
        `Unknown IQA model: ${newConfig.machineLearning.iqa.modelName}. Please check the model name for typos and confirm this is a supported model.`,
      );
    }
  }

  private async init(newConfig: SystemConfig, oldConfig?: SystemConfig) {
    if (this.worker !== ImmichWorker.MICROSERVICES || !isQualityAssessmentEnabled(newConfig.machineLearning)) {
      return;
    }

    await this.databaseRepository.withLock(DatabaseLock.IQAScore, async () => {
      const modelChange =
        oldConfig && oldConfig.machineLearning.iqa.modelName !== newConfig.machineLearning.iqa.modelName;

      if (!modelChange) {
        return;
      }

      const { isPaused } = await this.jobRepository.getQueueStatus(QueueName.IQA_SCORE);
      if (!isPaused) {
        await this.jobRepository.pause(QueueName.IQA_SCORE);
      }
      await this.jobRepository.waitForQueueCompletion(QueueName.IQA_SCORE);

      this.logger.log(
        `Quality check configuration changed (${oldConfig.machineLearning.iqa.modelName} ->  ${newConfig.machineLearning.iqa.modelName}), requeueing all assets for quality scoring`,
      );
      await this.qualityAssessmentRepository.clearAllIQAScores();

      if (!isPaused) {
        await this.jobRepository.resume(QueueName.IQA_SCORE);
      }
    });
  }

  @OnJob({ name: JobName.QUEUE_IQA_SCORE, queue: QueueName.IQA_SCORE })
  async handleQueueQualityAssessmentReport({ force }: JobOf<JobName.QUEUE_IQA_SCORE>): Promise<JobStatus> {
    const { machineLearning } = await this.getConfig({ withCache: false });
    if (!isQualityAssessmentEnabled(machineLearning)) {
      return JobStatus.SKIPPED;
    }

    if (force) {
      await this.qualityAssessmentRepository.clearAllIQAScores();
    }

    const assetPagination = usePagination(JOBS_ASSET_PAGINATION_SIZE, (pagination) => {
      return force
        ? this.assetRepository.getAll(pagination, { isVisible: true })
        : this.assetRepository.getWithout(pagination, WithoutProperty.QUALITY_ASSESSMENT);
    });

    for await (const assets of assetPagination) {
      await this.jobRepository.queueAll(assets.map((asset) => ({ name: JobName.IQA_SCORE, data: { id: asset.id } })));
    }

    return JobStatus.SUCCESS;
  }

  @OnJob({ name: JobName.IQA_SCORE, queue: QueueName.IQA_SCORE })
  async handleQualityAssessmentReport({ id }: JobOf<JobName.IQA_SCORE>): Promise<JobStatus> {
    const { machineLearning } = await this.getConfig({ withCache: true });
    if (!isQualityAssessmentEnabled(machineLearning)) {
      return JobStatus.SKIPPED;
    }

    const [asset] = await this.assetRepository.getByIds([id], { files: true });
    if (!asset) {
      return JobStatus.FAILED;
    }

    if (!asset.isVisible) {
      return JobStatus.SKIPPED;
    }

    const { previewFile } = getAssetFiles(asset.files);
    if (!previewFile) {
      return JobStatus.FAILED;
    }

    if (this.databaseRepository.isBusy(DatabaseLock.IQAScore)) {
      this.logger.verbose('Waiting for quality check configuration to be updated');
      await this.databaseRepository.wait(DatabaseLock.IQAScore);
    }

    try {
      const score = await this.machineLearningRepository.scoreImage(
        machineLearning.urls,
        previewFile.path,
        machineLearning.iqa,
      );

      await this.qualityAssessmentRepository.upsert(asset.id, score);

      return JobStatus.SUCCESS;
    } catch (error) {
      this.logger.error(`Failed to generate quality score for asset ${id}: ${error}`);
      return JobStatus.FAILED;
    }
  }
}
