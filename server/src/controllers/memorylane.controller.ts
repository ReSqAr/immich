import {Body, Controller, Get, Param, Post, Query} from '@nestjs/common';
import {ApiTags} from '@nestjs/swagger';
import {AuthDto} from 'src/dtos/auth.dto';
import {MemorylaneParamDto, MemorylaneQueryDto, MemorylaneResponseDto, MemorylanesBodyDto} from 'src/dtos/memorylane.dto';
import {Permission} from 'src/enum';
import {Auth, Authenticated} from 'src/middleware/auth.guard';
import {MemorylaneService} from 'src/services/memorylane.service';

@ApiTags('MemoryLane')
@Controller('memorylane')
export class MemoryLaneController {
  constructor(private service: MemorylaneService) {}

  @Get(':id')
  @Authenticated({ permission: Permission.MEMORY_READ })
  getMemoryLane(
    @Auth() auth: AuthDto,
    @Param() { id }: MemorylaneParamDto,
    @Query() { limit, type }: MemorylaneQueryDto,
  ): Promise<MemorylaneResponseDto> {
    return this.service.get(auth, type, id, limit);
  }

  @Post()
  @Authenticated({ permission: Permission.MEMORY_READ })
  getMemoryLanes(@Auth() auth: AuthDto, @Body() dto: MemorylanesBodyDto): Promise<MemorylaneResponseDto[]> {
    return Promise.all(dto.requests.map(request =>
        this.service.get(auth, request.type, request.id, request.limit)
    ));
  }
}
