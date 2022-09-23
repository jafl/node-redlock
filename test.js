'use strict';

var assert = require('chai').assert;
var Redlock = require('./redlock');

(async function(){

await test('single-server: https://www.npmjs.com/package/redis', require('redis').createClient());
await test('single-server: https://www.npmjs.com/package/redis (string_numbers=true)', require('redis').createClient({string_numbers: true}));

/* istanbul ignore next */
async function test(name, client){
	client.on('error', e => console.log(e));
	await client.connect();

	var redlock = new Redlock([client], {
		retryCount: 2,
		retryDelay: 150,
		retryJitter: 50
	});

	var resourceString = 'Redlock:test:resource';
	var resourceArray = ['Redlock:test:resource1','Redlock:test:resource2'];
	var error = 'Redlock:test:error';

	function cleanResourceString() {
		return client.del(resourceString);
	}

	function cleanResourceArray() {
		return Promise.all(resourceArray.map(k => client.del(k)));
	}

	describe('Redlock: ' + name, function(){

		before(async function() {
			await client.sAdd(error, 'having a set here should cause a failure');
			await client.scriptFlush();
		});

		it('should throw an error if not passed any clients', function(){
			assert.throws(function(){
				new Redlock([], {
					retryCount: 2,
					retryDelay: 150,
					retryJitter: 0
				});
			});
		});

		it('emits a clientError event when a client error occurs', async function(){
			var emitted = 0;
			function test(err) {
				assert.isNotNull(err);
				emitted++;
			}
			redlock.on('clientError', test);
			let err;
			try {
				await redlock.lock(error, 200);
			} catch (e) {
				err = e;
			}
			assert.isNotNull(err);
			redlock.removeListener('clientError', test);
			assert.equal(emitted, 3 * redlock.servers.length);
		});

		it('supports custom script functions in options', function(){
			var opts = {
				lockScript: function(lockScript) { return lockScript + 'and 1'; },
				unlockScript: function(unlockScript) { return unlockScript + 'and 2'; },
				extendScript: function(extendScript) { return extendScript + 'and 3'; },
			};
			var customRedlock = new Redlock([client], opts);
			var i = 1;
			assert.equal(customRedlock.lockScript, redlock.lockScript + 'and ' + i++);
			assert.equal(customRedlock.unlockScript, redlock.unlockScript + 'and ' + i++);
			assert.equal(customRedlock.extendScript, redlock.extendScript + 'and ' + i++);
		});

		describe('promises', function(){
			before(cleanResourceString);

			var one;
			it('should lock a resource', async function() {
				const lock = await redlock.lock(resourceString, 200);
				assert.isObject(lock);
				assert.isAbove(lock.expiration, Date.now()-1);
				assert.equal(lock.attempts, 1);
				one = lock;
			});

			var two;
			var two_expiration;
			it('should wait until a lock expires before issuing another lock', async function() {
				assert(one, 'Could not run because a required previous test failed.');
				const lock = await redlock.lock(resourceString, 800);
				assert.isObject(lock);
				assert.isAbove(lock.expiration, Date.now()-1);
				assert.isAbove(Date.now()+1, one.expiration);
				assert.isAbove(lock.attempts, 1);
				two = lock;
				two_expiration = lock.expiration;
			});

			it('should unlock a resource', async function() {
				assert(two, 'Could not run because a required previous test failed.');
				await two.unlock();
			});

			it('should unlock an already-unlocked resource', async function() {
				assert(two, 'Could not run because a required previous test failed.');
				try {
					await two.unlock();
					throw Error('Expecting LockError');
				} catch (e) {
					assert.instanceOf(e, Redlock.LockError);
				}
			});

			it('should error when unable to fully release a resource', async function() {
				assert(two, 'Could not run because a required previous test failed.');
				var failingTwo = Object.create(two);
				failingTwo.resource = error;
				try {
					await failingTwo.unlock();
					throw Error('Expecting LockError');
				} catch (e) {
					assert.instanceOf(e, Redlock.LockError);
				}
			});

			it('should fail to extend a lock on an already-unlocked resource', async function() {
				assert(two, 'Could not run because a required previous test failed.');
				try {
					await two.extend(200);
					throw Error('Expecting LockError');
				} catch (e) {
					assert.instanceOf(e, Redlock.LockError);
					assert.equal(e.attempts, 0);
				}
			});

			var three;
			it('should issue another lock immediately after a resource is unlocked', async function() {
				assert(two_expiration, 'Could not run because a required previous test failed.');
				const lock = await redlock.lock(resourceString, 800);
				assert.isObject(lock);
				assert.isAbove(lock.expiration, Date.now()-1);
				assert.isBelow(Date.now()-1, two_expiration);
				assert.equal(lock.attempts, 1);
				three = lock;
			});

			var four;
			it('should extend an unexpired lock', async function() {
				assert(three, 'Could not run because a required previous test failed.');
				const lock = await three.extend(800);
				assert.isObject(lock);
				assert.isAbove(lock.expiration, Date.now()-1);
				assert.isAbove(lock.expiration, three.expiration-1);
				assert.equal(lock.attempts, 1);
				assert.equal(three, lock);
				four = lock;
			});

			it('should fail after the maximum retry count is exceeded', async function() {
				assert(four, 'Could not run because a required previous test failed.');
				try {
					await redlock.lock(resourceString, 200)
					throw Error('Expecting LockError');
				} catch (e) {
					assert.instanceOf(e, Redlock.LockError);
					assert.equal(e.attempts, 3);
				}
			});

			it('should fail to extend an expired lock', function(done) {
				assert(four, 'Could not run because a required previous test failed.');
				setTimeout(async function(){
					try {
						await three.extend(800);
						done(Error('Expecting LockError'));
					} catch (e) {
						assert.instanceOf(e, Redlock.LockError);
						assert.equal(e.attempts, 0);
						done();
					}
				}, four.expiration - Date.now() + 100);
			});

			after(cleanResourceString);
		});

		describe('promises - multi', function(){
			before(cleanResourceArray);

			var one;
			it('should lock a multivalue resource', async function() {
				const lock = await redlock.lock(resourceArray, 200);
				assert.isObject(lock);
				assert.isAbove(lock.expiration, Date.now()-1);
				assert.equal(lock.attempts, 1);
				one = lock;
			});

			var two;
			var two_expiration;
			it('should wait until a multivalue lock expires before issuing another lock', async function() {
				assert(one, 'Could not run because a required previous test failed.');
				const lock = await redlock.lock(resourceArray, 800);
				assert.isObject(lock);
				assert.isAbove(lock.expiration, Date.now()-1);
				assert.isAbove(Date.now()+1, one.expiration);
				assert.isAbove(lock.attempts, 1);
				two = lock;
				two_expiration = lock.expiration;
			});

			it('should unlock a multivalue resource', async function() {
				assert(two, 'Could not run because a required previous test failed.');
				await two.unlock();
			});

			it('should unlock an already-unlocked multivalue resource', async function() {
				assert(two, 'Could not run because a required previous test failed.');
				try {
					await two.unlock()
					throw Error('Expecting LockError');
				} catch (e) {
					assert.instanceOf(e, Redlock.LockError);
				}
			});

			it('should error when unable to fully release a multivalue resource', async function() {
				assert(two, 'Could not run because a required previous test failed.');
				var failingTwo = Object.create(two);
				failingTwo.resource = error;
				try {
					await failingTwo.unlock();
					throw Error('Expecting LockError');
				} catch (e) {
					assert.instanceOf(e, Redlock.LockError);
				}
			});

			it('should fail to extend a lock on an already-unlocked multivalue resource', async function() {
				assert(two, 'Could not run because a required previous test failed.');
				try {
					await two.extend(200);
					throw Error('Expecting LockError');
				} catch (e) {
					assert.instanceOf(e, Redlock.LockError);
					assert.equal(e.attempts, 0);
				}
			});

			var three;
			it('should issue another lock immediately after a multivalue resource is unlocked', async function() {
				assert(two_expiration, 'Could not run because a required previous test failed.');
				const lock = await redlock.lock(resourceArray, 800);
				assert.isObject(lock);
				assert.isAbove(lock.expiration, Date.now()-1);
				assert.isBelow(Date.now()-1, two_expiration);
				assert.equal(lock.attempts, 1);
				three = lock;
			});

			var four;
			it('should extend an unexpired lock', async function() {
				assert(three, 'Could not run because a required previous test failed.');
				const lock = await three.extend(800);
				assert.isObject(lock);
				assert.isAbove(lock.expiration, Date.now()-1);
				assert.isAbove(lock.expiration, three.expiration-1);
				assert.equal(lock.attempts, 1);
				assert.equal(three, lock);
				four = lock;
			});

			it('should fail after the maximum retry count is exceeded', async function() {
				assert(four, 'Could not run because a required previous test failed.');
				try {
					await redlock.lock(resourceArray, 200);
					throw Error('Expecting LockError');
				} catch (e) {
					assert.instanceOf(e, Redlock.LockError);
					assert.equal(e.attempts, 3);
				}
			});

			it('should fail to extend an expired lock', function(done) {
				assert(four, 'Could not run because a required previous test failed.');
				setTimeout(async function(){
					try {
						await three.extend(800);
						done(Error('Expecting LockError'));
					} catch (e) {
						assert.instanceOf(e, Redlock.LockError);
						assert.equal(e.attempts, 0);
						done();
					}
				}, four.expiration - Date.now() + 100);
			});

			after(cleanResourceArray);
		});

		describe('quit', function() {
			it('should quit all clients', async function(){
				const results = await redlock.quit();
				assert.isArray(results);
			});
		})

	});
}

run();

})();
