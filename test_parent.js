'use strict';

const s2             = require('s2');

let DATA = {};
const EARTH_RADIUS = 6371;

const PARENT_ZOOMS = [
    14, 15, 16
];

let PARENTS = {};

const doOnce = fCallback => {
    const N = 100000;
    const center    = [-73.993549, 40.727248];
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

        DATA[id.id()] = {id: id.id(), latitude: incidentLat, longitude: incidentLon, ts: tPrevious};

        // Adding the data to multiple buckets at different zoom levels, which amounts to x copies of our data
        PARENT_ZOOMS.map(iZoom => {
            if (!PARENTS[iZoom]) {
                PARENTS[iZoom] = {};
            }

            const parent_id = id.parent(iZoom).id();
            if (!PARENTS[iZoom][parent_id]) {
                PARENTS[iZoom][parent_id] = [];
            }

            PARENTS[iZoom][parent_id].push(id.id());
        });

    }

    fCallback();
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

    const oSettings   = {
        min:         PARENT_ZOOMS[0],
        max:         PARENT_ZOOMS[PARENT_ZOOMS.length - 1],
        max_cells:   50,
        result_type: 'cellId'
    };

    s2.getCover(oCap, oSettings, (oError, aCoverIds) => {
        console.log({action: 'nearby.cover', dT: Date.now() - t0, ranges: aCoverIds.length});

        let aMatches = [];
        const t2 = Date.now();
        aCoverIds.map(id => {
            // We have a matching parent zone.  Just return all items in that matching zone and don't bother filtering
            if (PARENTS[id.level()] && PARENTS[id.level()][id.id()]) {
                aMatches = aMatches.concat([], PARENTS[id.level()][id.id()]);
            } else {
                console.log('missing parent', id.level());
            }
        });
        console.log({action: 'nearby.filters', dT: Date.now() - t0, oT: Date.now() - t2});
        console.log({
            action: 'nearby.visualize',
            url:    `https://s2.sidewalklabs.com/regioncoverer/?center=${nLatitude},${nLongitude}&zoom=15&cells=${aCoverIds.map(oId => oId.toToken()).join(',')}`
        });

        fNearbyCallback(aMatches);
    })
};

doOnce(() => {
    const t0 = Date.now();
    nearby(40.727248, -73.993549, 1000, aKeys => {
        console.log({action: 'results.keys', dT: Date.now() - t0, count: aKeys.length});

        const t3 = Date.now();
        const aMatches = aKeys.map(iKey => DATA[iKey]);

        console.log({action: 'results.nearby', dT: Date.now() - t0, oT: Date.now() - t3});

        const t4 = Date.now();
        aMatches.sort((a, b) => b.ts - a.ts);

        // This one seems to take a lot longer to sort
        let aSorted = aMatches.slice(0, 50);

        console.log({action: 'results.sorted', dT: Date.now() - t0, oT: Date.now() - t4});
    });
});



