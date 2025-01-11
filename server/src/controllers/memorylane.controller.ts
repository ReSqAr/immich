import { Body, Controller, Get, Param, Post, Query } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { AuthDto } from 'src/dtos/auth.dto';
import {
  MemorylaneParamDto,
  MemorylaneQueryDto,
  MemorylaneResponseDto,
  MemorylanesBodyDto,
} from 'src/dtos/memorylane.dto';
import { Permission } from 'src/enum';
import { Auth, Authenticated } from 'src/middleware/auth.guard';
import { MemorylaneService } from 'src/services/memorylane.service';

@ApiTags('MemoryLane')
@Controller('memorylane')
export class MemoryLaneController {
  constructor(private service: MemorylaneService) {}

  @Get(':id')
  @Authenticated({ permission: Permission.MEMORY_READ })
  async getMemoryLane(
    @Auth() auth: AuthDto,
    @Param() { id }: MemorylaneParamDto,
    @Query() { limit, type }: MemorylaneQueryDto,
  ): Promise<MemorylaneResponseDto | undefined> {
    return await this.service.get(auth, type, id, limit);
  }

  @Post()
  @Authenticated({ permission: Permission.MEMORY_READ })
  async getMemoryLanes(@Auth() auth: AuthDto, @Body() dto: MemorylanesBodyDto): Promise<MemorylaneResponseDto[]> {
    const results = await Promise.all(
      dto.requests.map((request) => this.service.get(auth, request.type, request.id, request.limit)),
    );
    return results.filter((value): value is MemorylaneResponseDto => value !== undefined);
  }
}
