'use strict';

const s2    = require('s2');
const async = require('async');

const EARTH_RADIUS = 6371;

let KEYS = new Set();
let DATA = {};

const doOnce = (fCallback) => {
    const N = 100000;
    const lowerLeft = [-74.009180, 40.716425];
    const deltaLon  = Math.abs(lowerLeft[0] - (-73.97725));
    const deltaLat  = Math.abs(lowerLeft[1] - (40.7518692));
    let   tPrevious = 1475431264754;

    for (let i = 0; i < N; i++) {
        const incidentLon = lowerLeft[0] + Math.random() * deltaLon;
        const incidentLat = lowerLeft[1] + Math.random() * deltaLat;
        tPrevious        += Math.random() * 60 * 1000; // random time after previous
        const ll          = new s2.S2LatLng(incidentLat, incidentLon);
        const id          = new s2.S2CellId(ll.normalized());

        KEYS.add(id.id());
        DATA[id.id()] = {id: id.id(), latitude: incidentLat, longitude: incidentLon, ts: tPrevious};
    }

    fCallback();
};

function radius2height(iRadius) {
    return 1 - Math.sqrt(1 - Math.pow((iRadius / EARTH_RADIUS), 2));
}

const nearby = (nLatitude, nLongitude, iRadiusMeters, fNearbyCallback) => {
    const t0 = Date.now();
    const oLatLong  = new s2.S2LatLng(nLatitude, nLongitude);

    console.log({action: 'nearby.ll', dT: Date.now() - t0});

    // Spherical Cap - Slice the Earth by Radius!
    // Got this from the LevelDB Demo: https://github.com/gerhardberger/level-nearby/blob/master/index.js
    const oCap = new s2.S2Cap(oLatLong.normalized().toPoint(), radius2height(iRadiusMeters / 1000));
    
    console.log({action: 'nearby.cap', dT: Date.now() - t0});

    // Switch these around to play with different block sizes.  Check out the "Visualize" link in the console output to see what all this means
    const oSettingsSpeedy     = {min: 10, max: 15, max_cells: 35,    result_type: 'cellId'};
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

        const t2 = Date.now();
        let aMatches = [...KEYS].filter(iKey => aRanges.filter(oRange => iKey >= oRange.min && iKey <= oRange.max).length > 0);

        console.log({action: 'nearby.filters', dT: Date.now() - t0, oT: Date.now() - t2, filtered: aMatches.length});

        fNearbyCallback(aMatches); // Concat and Unique
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