'use strict';

const s2    = require('s2');
const redis = require('redis');
const url   = require('url');

const SKIP_DATA_GENERATION = true;
const EARTH_RADIUS = 6371;

let DATA = {};


const getRedisClient = () => {
    const redisUrl = url.parse('redis://localhost:6379');
    const client = redis.createClient(redisUrl.port, redisUrl.hostname);
    // console.log({ action: 'app', redisUrl: redisUrl });
    if (redisUrl.auth) {
        client.auth(redisUrl.auth.split(":")[1]);
    }
    return client;
};


const oClient = getRedisClient();

const doOnce = fCallback => {
    if (SKIP_DATA_GENERATION) {
        return fCallback();
    }

    console.log({action: 'data.flush'});
    oClient.flushall();

    console.log({action: 'data.generating'});
    const N = 100000;
    const center    = [-73.993549, 40.727248];
    const lowerLeft = [-74.009180, 40.716425];
    const deltaLon  = Math.abs(lowerLeft[0] - (-73.97725));
    const deltaLat  = Math.abs(lowerLeft[1] - (40.7518692));
    let   tPrevious = 1475431264754;

    let aData = ['incident_locations'];  // USING a Sorted Set

    // I haven't worked with redis much, which is to say there are probably far better ways to do things
    // I'm using a sorted set to store our ids.

    for (let i = 0; i < N; i++) {
        const incidentLon = lowerLeft[0] + Math.random() * deltaLon;
        const incidentLat = lowerLeft[1] + Math.random() * deltaLat;
        tPrevious        += Math.random() * 60 * 1000; // random time after previous
        const ll          = new s2.S2LatLng(incidentLat, incidentLon);
        const id          = new s2.S2CellId(ll.normalized());

        // In our case, the ID IS the score, which is why we're adding it twice - once as the id and once as the score.
        aData.push(id.id());
        aData.push(id.id());

        // The actual data that we're going to retreive once we have our ids - should probably be in redis.  It's not, yet.
        DATA[id.id()] = {id: id.id(), latitude: incidentLat, longitude: incidentLon, ts: tPrevious};
    }

    oClient.zadd(aData, (err, res) => {
        if (err) {
            console.error({action: 'data.error', error: err, res: res});
        } else {
            console.log({action: 'data.done'});
            fCallback();
        }
    })
};

function radius2height(iRadius) {
    return 1 - Math.sqrt(1 - Math.pow((iRadius / EARTH_RADIUS), 2));
}

const nearby = (nLatitude, nLongitude, iRadiusMeters, fNearbyCallback) => {
    const t0 = Date.now();

    const oLatLong = new s2.S2LatLng(nLatitude, nLongitude);

    console.log({action: 'nearby.ll', dT: Date.now() - t0});

    // Spherical Cap - Slice the Earth by Radius!
    // Got this from the LevelDB Demo: https://github.com/gerhardberger/level-nearby/blob/master/index.js
    const oCap = new s2.S2Cap(oLatLong.normalized().toPoint(), radius2height(iRadiusMeters / 1000));

    console.log({action: 'nearby.cap', dT: Date.now() - t0});
    const ranges = [];

    // Switch these around to play with different block sizes.  Check out the "Visualize" link in the console output to see what all this means
    const oSettingsSpeedy = {min: 1, max: 13, max_cells: 35, result_type: 'cellId'};
    const oSettingsReasonable = {min: 1, max: 14, max_cells: 20, result_type: 'cellId'};
    const oSettingsPrecise = {min: 1, max: 30, max_cells: 10000, result_type: 'cellId'};

    s2.getCover(oCap, oSettingsSpeedy, (oError, aCoverIds) => {
        console.log({action: 'nearby.cover', dT: Date.now() - t0, ranges: aCoverIds.length});

        const aRanges = aCoverIds.map(oId => {
            return {
                min: oId.rangeMin().id(),
                max: oId.rangeMax().id()
            };
        });

        console.log({action: 'nearby.ranges', dT: Date.now() - t0});
        console.log({
            action: 'nearby.visualize',
            url:    `https://s2.sidewalklabs.com/regioncoverer/?center=${nLatitude},${nLongitude}&zoom=15&cells=${aCoverIds.map(oId => oId.toToken()).join(',')}`
        });

        let iResponses = 0;
        let aMatches = [];

        const t1 = Date.now();
        aRanges.map(oRange => {
            const args1 = ['incident_locations', oRange.min, oRange.max];

            const t2 = Date.now();
            oClient.zrangebyscore(args1, function (err, aResponse) {
                console.log({action: 'nearby.filter', dT: Date.now() - t0, oT: Date.now() - t2});
                iResponses++;
                if (err) throw err;

                aMatches = aMatches.concat([], aResponse);

                if (iResponses >= aRanges.length) {
                    oClient.quit();

                    console.log({action: 'nearby.filters', dT: Date.now() - t0, oT: Date.now() - t1});
                    fNearbyCallback(aMatches);
                }
            });
        });
    })
};

doOnce(() => {
    const t0 = Date.now();
    nearby(40.727248, -73.993549, 1000, aKeys => {
        console.log({action: 'results.keys', dT: Date.now() - t0, count: aKeys.length});

        const aMatches = aKeys.map(iKey => DATA[iKey]);

        console.log({action: 'results.nearby', dT: Date.now() - t0});

        aMatches.sort((a, b) => b.ts - a.ts);

        let aSorted = aMatches.slice(0, 50);

        console.log({action: 'results.sorted', dT: Date.now() - t0});
    });
});
