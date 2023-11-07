import { Controller, Get, Param, Req } from '@nestjs/common';

@Controller('search')
export class SearchController {

    @Get('/channel/:id')
    getChannelInfo(@Param('id') channelId: string){
        return channelId;
    }
}
