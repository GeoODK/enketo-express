'use strict';

var Promise = require( 'lie' );
var config = require( './config-model' ).server;
var TError = require( '../lib/custom-error' ).TranslatedError;
var client = require( 'redis' ).createClient( config.redis.main.port, config.redis.main.host, {
    auth_pass: config.redis.main.password
} );
var debug = require( 'debug' )( 'submission-model' );
var logger;
var winston;

// only instantiate logger if required
if ( config.log.submissions ) {
    winston = require( 'winston' );
    logger = new winston.Logger( {
        transports: [
            new winston.transports.File( {
                level: 'info',
                filename: 'logs/submissions.log',
                json: false,
                maxsize: 200, //50 * 1024 * 1024,
                colorize: false,
                showLevel: false,
                zippedArchive: true,
                timestamp: false,
                formatter: _formatter
            } )
        ],
        exitOnError: false
    } );
}

// in test environment, switch to different db
if ( process.env.NODE_ENV === 'test' ) {
    client.select( 15 );
}


/**
 * Whether instanceID was submitted successfully before.
 *
 * To prevent large submissions that were divided into multiple batches from recording multiple times,
 * we use a redis capped list to store the latest 100 instanceIDs
 * This list can be queried to avoid double-counting instanceIDs
 *
 * Note that edited records are submitted multiple times with different instanceIDs.
 *
 * @param  {[type]}  id         [description]
 * @param  {[type]}  instanceId [description]
 * @return {Boolean}            [description]
 */
function isNew( id, instanceId ) {
    var key;
    var error;

    if ( !id || !instanceId ) {
        error = new Error( 'Cannot log instanceID: either enketoId or instanceID not provided', id, instanceId );
        error.status = 400;
        return Promise.reject( error );
    }

    key = 'su:' + id.trim();

    return _getLatestSubmissionIds( key )
        .then( function( latest ) {
            return _alreadyRecorded( instanceId, latest );
        } )
        .then( function( alreadyRecorded ) {
            if ( !alreadyRecorded ) {
                client.lpush( key, instanceId, function( error, res ) {
                    if ( error ) {
                        console.error( 'Error pushing instanceID into: ' + key );
                    } else {
                        // only store last 100 IDs
                        client.ltrim( key, 0, 99, function( error, res ) {
                            if ( error ) {
                                console.error( 'Error trimming: ' + key );
                            }
                        } );
                    }
                } );
                return true;
            }
            return false;
        } );
}

function add( id, instanceId, deprecatedId ) {
    if ( logger ) {
        logger.info( instanceId, {
            enketoId: id,
            deprecatedId: deprecatedId
        } );
    }
}


function _alreadyRecorded( instanceId, list ) {
    list = list || [];
    return list.indexOf( instanceId ) !== -1;
}

function _getLatestSubmissionIds( key ) {
    return new Promise( function( resolve, reject ) {
        client.lrange( key, 0, -1, function( error, res ) {
            if ( error ) {
                reject( error );
            } else {
                resolve( res );
            }
        } );
    } );
}

function _formatter( args ) {
    return [ new Date().toISOString(), args.message, args.meta.enketoId, args.meta.deprecatedId ].join( ',' );
}

module.exports = {
    isNew: isNew,
    add: add
};
