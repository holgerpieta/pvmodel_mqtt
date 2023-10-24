import logging
logging.basicConfig(level=logging.INFO)

import sys
import asyncio
import json
import aiomqtt
import pvlib
import pandas as pd

if 'win32' in sys.platform:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

loc = pvlib.location.Location(50.8078105, 6.2620244, 'Europe/Berlin', 166, 'Eschweiler')
base_topic = 'pv_model'
will_topic = base_topic + '/status'
ghi_topic = base_topic + '/ghi'
irad_topic = base_topic + '/irad'

async def handle_ghi(client, message):
    try:
        inp = json.loads(message.payload)
    except json.JSONDecodeError as e:
        logging.error('Failed to decode payload:', e )
        return
    if 'time' not in inp:
        logging.error('Time missing in input.')
        return
    if 'ghi' not in inp:
        logging.error('GHI missing in input.')
        return
    if 'tilt' not in inp:
        logging.error('Tilt missing in input.')
        return
    if 'azimuth' not in inp:
        logging.error('Azimuth missing in input.')
        return
    if len(inp['time']) != len(inp['ghi']):
        logging.error('List length mismatch.')
        return
    # time = pd.date_range(start=pd.Timestamp(inp['time']), end=pd.Timestamp(inp['time']), periods=1)
    # time = pd.date_range(start=pd.Timestamp('13:00', tz='Europe/Berlin'), end=pd.Timestamp('13:00', tz='Europe/Berlin'), periods=1)
    time = pd.DatetimeIndex(pd.Timestamp(t) for t in inp['time'])
    logging.debug('Time:\n' + str(time))
    ghi = pd.Series((float(x) for x in inp['ghi']), index=time, name='ghi')
    logging.debug('GHI:\n' + ghi.to_string())
    solpos = loc.get_solarposition(time)
    logging.debug('Sun Position:\n' + solpos.to_string())
    cs = loc.get_clearsky(time)
    cs.ghi[cs.ghi.isnull()] = 0.
    cs.dni[cs.dni.isnull()] = 0.
    cs.dhi[cs.dhi.isnull()] = 0.
    logging.debug('Clear Sky:\n' + cs.to_string())
    dni = pvlib.irradiance.dirindex(ghi, cs.ghi, cs.dni, solpos.zenith, time)
    dni[dni.isnull()] = 0.
    logging.debug('DNI:\n' + dni.to_string())
    irad = pvlib.irradiance.complete_irradiance(solpos.zenith, ghi=ghi, dni=dni)
    irad.ghi[irad.ghi.isnull()] = 0.
    irad.dni[irad.dni.isnull()] = 0.
    irad.dhi[irad.dhi.isnull()] = 0.
    logging.debug('Irradiation:\n' + irad.to_string())
    irad_panel = pvlib.irradiance.get_total_irradiance(inp['tilt'], inp['azimuth'], solpos.zenith, solpos.azimuth, irad.dni, irad.ghi, irad.dhi, surface_type='urban')
    logging.debug('Panel Irradiation:\n' + irad_panel.to_string())
    payload = json.dumps({
        'sun_elevation': solpos.elevation.tolist(),
        'sun_azimuth': solpos.azimuth.tolist(),
        'sun_zenith': solpos.zenith.tolist(),
        'clearsky_ghi': cs.ghi.tolist(),
        'clearsky_dni': cs.dni.tolist(),
        'clearsky_dhi': cs.dhi.tolist(),
        'global_ghi': irad.ghi.tolist(),
        'global_dni': irad.dni.tolist(),
        'global_dhi': irad.dhi.tolist(),
        'panel_total': irad_panel.poa_global.tolist(),
        'panel_direct': irad_panel.poa_direct.tolist(),
        'panel_diffuse': irad_panel.poa_diffuse.tolist(),
        'panel_sky_diffuse': irad_panel.poa_sky_diffuse.tolist(),
        'panel_ground_diffuse': irad_panel.poa_ground_diffuse.tolist(),
    })
    await client.publish(irad_topic, payload, 2)

async def main():
    async with asyncio.TaskGroup() as tg:
        async with aiomqtt.Client(
            hostname='raspberrypi.fritz.box',
            client_id='pv_model',
            will=aiomqtt.Will(will_topic, 'offline', 2, True),
            
        ) as client:
            async with client.messages() as messages:
                await client.publish(will_topic, 'online', 2, True)
                await client.subscribe(ghi_topic, 2)
                async for message in messages:
                    if message.topic.matches(ghi_topic):
                        tg.create_task(handle_ghi(client, message))

asyncio.run(main())
