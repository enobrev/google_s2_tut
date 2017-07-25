'use strict';

const process = require('process');
const s2      = require('s2');
const mysql   = require('mysql');
const url     = require('url');

const SKIP_DATA_GENERATION = true;
const EARTH_RADIUS         = 6371;

const oClient = mysql.createConnection({
    host:     '192.168.1.2',
    user:     'dev',
    password: 'dev',
    database: 'dev_citizen',
    multipleStatements: true
});

/* Here's the database:

 CREATE TABLE `locations` (
 `location_id` varchar(60) NOT NULL,
 `location_latitude` decimal(11,8) DEFAULT NULL,
 `location_longitude` decimal(11,8) DEFAULT NULL,
 `location_date_created` datetime NOT NULL
 ) ENGINE=MEMORY DEFAULT CHARSET=utf8mb4;

 ALTER TABLE `locations` ADD PRIMARY KEY (`location_id`,`location_date_created`) USING HASH;

 */

oClient.connect();

const doOnce = fCallback => {
    if (SKIP_DATA_GENERATION) {
        return fCallback();
    }

    console.log({action: 'data.flush'});
    oClient.query(`DELETE FROM locations`, (error, results, fields)=> {
        if (error) {
            console.error(error);
            process.exit();
        }

        console.log({action: 'data.generating'});
        const N = 100000;
        const center    = [-73.993549, 40.727248];
        const lowerLeft = [-74.009180, 40.716425];
        const deltaLon  = Math.abs(lowerLeft[0] - (-73.97725));
        const deltaLat  = Math.abs(lowerLeft[1] - (40.7518692));
        let   tPrevious = 1475431264754;
        let aQueries = [];
        let aValues  = [];

        for (let i = 0; i < N; i++) {
            const incidentLon = lowerLeft[0] + Math.random() * deltaLon;
            const incidentLat = lowerLeft[1] + Math.random() * deltaLat;
            tPrevious        += Math.random() * 60 * 1000; // random time after previous
            const ll          = new s2.S2LatLng(incidentLat, incidentLon);
            const id          = new s2.S2CellId(ll.normalized());

            const oValues = {
                location_id:            id.id(),
                location_latitude:      incidentLat,
                location_longitude:     incidentLon,
                location_date_created:  new Date()
            };

            aQueries.push('INSERT IGNORE INTO locations SET ?');
            aValues.push(oValues);
        }

        console.log({action: 'data.import', count: aQueries.length});

        const iChunk   = 10000;
        const iQueries = Math.floor(aQueries.length / iChunk);
        let iDone = 0;
        let i;
        let j;
        for (i = 0, j = aQueries.length; i < j; i += iChunk) {
            const aQueryChunk = aQueries.slice(i, i + iChunk);
            const aValueChunk = aValues.slice(i,  i + iChunk);

            oClient.query(aQueryChunk.join('; '), aValueChunk, (oError, oResults) => {
                if (oError) {
                    console.error(oError);
                    process.exit();
                }
                iDone++;
                console.log({action: 'data.done', number: iDone, of: iQueries});

                if (iDone >= iQueries) {
                    fCallback();
                }
            });
        }
    });

};

function radius2height(iRadius) {
    return 1 - Math.sqrt(1 - Math.pow((iRadius / EARTH_RADIUS), 2));
}

function height2radius(height) {
    return Math.sqrt(1 - Math.pow(1 - height, 2)) * EARTH_RADIUS;
}

const nearby = (nLatitude, nLongitude, iRadiusMeters, fNearbyCallback) => {
    const t0 = Date.now();

    const oLatLong  = new s2.S2LatLng(nLatitude, nLongitude);

    console.log({action: 'nearby.ll', dT: Date.now() - t0});

    // Spherical Cap - Slice the Earth by Radius!
    // Got this from the LevelDB Demo: https://github.com/gerhardberger/level-nearby/blob/master/index.js
    const oCap = new s2.S2Cap(oLatLong.normalized().toPoint(), radius2height(iRadiusMeters / 1000));

    console.log({action: 'nearby.cap', dT: Date.now() - t0});
    const ranges = [];

    // Switch these around to play with different block sizes.  Check out the "Visualize" link in the console output to see what all this means
    const oSettingsSpeedy     = {min: 1,  max: 13, max_cells: 35,    result_type: 'cellId'};
    const oSettingsReasonable = {min: 1,  max: 14, max_cells: 20,    result_type: 'cellId'};
    const oSettingsPrecise    = {min: 1,  max: 30, max_cells: 10000, result_type: 'cellId'};

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

        let aConditions = [];
        aRanges.map(oRange => {
            aConditions.push(`location_id BETWEEN ${oRange.min} AND ${oRange.max}`);
            //aConditions.push(`location_id >= ${range.min} AND location_id <= ${range.max}`); // SLOWER
        });

        console.log({action: 'nearby.conditions', dT: Date.now() - t0});

        const t1   = Date.now();
        const sSQL = `SELECT * FROM locations WHERE ${aConditions.join(' OR ')} ORDER BY location_date_created DESC LIMIT 50`;

        console.log({action: 'nearby.sql', dT: Date.now() - t0, sql: sSQL});

        oClient.query(sSQL, (oError, aResults) => {
            oClient.end();

            console.log({action: 'nearby.query', dT: Date.now() - t0, oT: Date.now() - t1});
            if (oError) throw oError;

            fNearbyCallback(aResults);
        });
    })
};

doOnce(() => {
    const t0 = Date.now();
    nearby(40.727248, -73.993549, 1000, data => {
        console.log({action: 'results.sorted', dT: Date.now() - t0, results: data.length});
    });
});
