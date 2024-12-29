import { SystemConfig } from 'src/config';
import { ImmichWorker } from 'src/enum';
import { IAssetRepository, WithoutProperty } from 'src/interfaces/asset.interface';
import { IConfigRepository } from 'src/interfaces/config.interface';
import { IDatabaseRepository } from 'src/interfaces/database.interface';
import { IJobRepository, JobName, JobStatus } from 'src/interfaces/job.interface';
import { IMachineLearningRepository } from 'src/interfaces/machine-learning.interface';
import { IQualityAssessmentRepository } from 'src/interfaces/quality-assessment.interface';
import { ISystemMetadataRepository } from 'src/interfaces/system-metadata.interface';
import { QualityAssessmentService } from 'src/services/quality-assessment.service';
import { assetStub } from 'test/fixtures/asset.stub';
import { systemConfigStub } from 'test/fixtures/system-config.stub';
import { newTestService } from 'test/utils';
import { Mocked } from 'vitest';

describe(QualityAssessmentService.name, () => {
  let sut: QualityAssessmentService;
  let assetMock: Mocked<IAssetRepository>;
  let databaseMock: Mocked<IDatabaseRepository>;
  let jobMock: Mocked<IJobRepository>;
  let machineLearningMock: Mocked<IMachineLearningRepository>;
  let configMock: Mocked<IConfigRepository>;
  let qualityAssessmentMock: Mocked<IQualityAssessmentRepository>;
  let systemMock: Mocked<ISystemMetadataRepository>;

  beforeEach(() => {
    ({ sut, assetMock, databaseMock, jobMock, qualityAssessmentMock, machineLearningMock, configMock, systemMock } =
      newTestService(QualityAssessmentService));

    assetMock.getByIds.mockResolvedValue([assetStub.image]);
    configMock.getWorker.mockReturnValue(ImmichWorker.MICROSERVICES);
    configMock.getEnv.mockReturnValue(systemConfigStub.machineLearningEnabled as any);
  });

  it('should work', () => {
    expect(sut).toBeDefined();
  });

  describe('onConfigValidate', () => {
    it('should validate IQA model name', () => {
      expect(() =>
        sut.onConfigValidate({
          newConfig: {
            machineLearning: {
              enabled: true,
              urls: ['http://test-url'],
              iqa: { modelName: 'invalid-model' },
            },
          } as SystemConfig,
          oldConfig: {} as SystemConfig,
        }),
      ).toThrow('Unknown IQA model: invalid-model');
    });
  });

  describe('onConfigInit', () => {
    it('should skip if not microservices worker', async () => {
      configMock.getWorker.mockReturnValue(ImmichWorker.API);
      await sut.onConfigInit({ newConfig: systemConfigStub.machineLearningEnabled as SystemConfig });

      expect(qualityAssessmentMock.clearAllIQAScores).not.toHaveBeenCalled();
      expect(jobMock.getQueueStatus).not.toHaveBeenCalled();
    });

    it('should skip if IQA is disabled', async () => {
      await sut.onConfigInit({ newConfig: systemConfigStub.machineLearningDisabled as SystemConfig });

      expect(qualityAssessmentMock.clearAllIQAScores).not.toHaveBeenCalled();
      expect(jobMock.getQueueStatus).not.toHaveBeenCalled();
    });

    it('should initialize with queue management', async () => {
      jobMock.getQueueStatus.mockResolvedValue({ isActive: false, isPaused: false });

      await sut.onConfigInit({
        newConfig: systemConfigStub.machineLearningEnabled as SystemConfig,
      });

      expect(databaseMock.withLock).toHaveBeenCalled();
    });
  });

  describe('handleQueueQualityGeneration', () => {
    it('should skip when IQA is disabled', async () => {
      // Override default config to disabled
      systemMock.get.mockResolvedValue(systemConfigStub.machineLearningDisabled);

      const result = await sut.handleQueueQualityAssessmentReport({ force: false });

      expect(result).toBe(JobStatus.SKIPPED);
      expect(assetMock.getAll).not.toHaveBeenCalled();
      expect(assetMock.getWithout).not.toHaveBeenCalled();
    });

    it('should queue assets without IQA scores', async () => {
      assetMock.getWithout.mockResolvedValue({
        items: [assetStub.image],
        hasNextPage: false,
      });

      const result = await sut.handleQueueQualityAssessmentReport({ force: false });

      expect(result).toBe(JobStatus.SUCCESS);
      expect(assetMock.getWithout).toHaveBeenCalledWith({ skip: 0, take: 1000 }, WithoutProperty.QUALITY_ASSESSMENT);
      expect(jobMock.queueAll).toHaveBeenCalledWith([{ name: JobName.IQA_SCORE, data: { id: assetStub.image.id } }]);
    });

    it('should handle force requeue', async () => {
      assetMock.getAll.mockResolvedValue({
        items: [assetStub.image],
        hasNextPage: false,
      });

      const result = await sut.handleQueueQualityAssessmentReport({ force: true });

      expect(result).toBe(JobStatus.SUCCESS);
      expect(qualityAssessmentMock.clearAllIQAScores).toHaveBeenCalled();
      expect(assetMock.getAll).toHaveBeenCalled();
    });
  });

  describe('handleQualityAssessmentReport', () => {
    it('should skip when IQA is disabled', async () => {
      systemMock.get.mockResolvedValue(systemConfigStub.machineLearningDisabled);

      const result = await sut.handleQualityAssessmentReport({ id: 'test-id' });

      expect(result).toBe(JobStatus.SKIPPED);
      expect(machineLearningMock.scoreImage).not.toHaveBeenCalled();
    });

    it('should fail when asset not found', async () => {
      assetMock.getByIds.mockResolvedValue([]);

      const result = await sut.handleQualityAssessmentReport({ id: 'test-id' });

      expect(result).toBe(JobStatus.FAILED);
      expect(machineLearningMock.scoreImage).not.toHaveBeenCalled();
    });

    it('should handle successful score generation', async () => {
      machineLearningMock.scoreImage.mockResolvedValue(0.85);

      const result = await sut.handleQualityAssessmentReport({ id: assetStub.image.id });

      expect(result).toBe(JobStatus.SUCCESS);
      expect(machineLearningMock.scoreImage).toHaveBeenCalled();
      expect(qualityAssessmentMock.upsert).toHaveBeenCalledWith(assetStub.image.id, 0.85);
    });

    it('should handle scoring errors', async () => {
      machineLearningMock.scoreImage.mockRejectedValue(new Error('Scoring failed'));

      const result = await sut.handleQualityAssessmentReport({ id: assetStub.image.id });

      expect(result).toBe(JobStatus.FAILED);
      expect(machineLearningMock.scoreImage).toHaveBeenCalled();
      expect(qualityAssessmentMock.upsert).not.toHaveBeenCalled();
    });

    it('should handle database locks', async () => {
      databaseMock.isBusy.mockReturnValue(true);
      machineLearningMock.scoreImage.mockResolvedValue(0.85);

      const result = await sut.handleQualityAssessmentReport({ id: assetStub.image.id });

      expect(result).toBe(JobStatus.SUCCESS);
      expect(databaseMock.wait).toHaveBeenCalled();
      expect(machineLearningMock.scoreImage).toHaveBeenCalled();
      expect(qualityAssessmentMock.upsert).toHaveBeenCalledWith(assetStub.image.id, 0.85);
    });
  });
});
