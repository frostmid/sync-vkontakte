var Slave = require ('fos-sync-slave'),
	SocketIO = require ('socket.io-client'),
	vkontakte = require ('vkontakte'),
	_ = require ('lodash'),
	Promises = require ('vow');

var getVkontakte = function (token) {
	// console.log (token.client_id, token.access_token);
	// return vkontakte (token.client_id, token.access_token);
	return vkontakte (token.access_token);
};


function normalize (entry, type) {
	function not_implemented (what) {
		return new Error ('Not implemented ' + what);
	}

	switch (entry.post_type || type) {
		case 'post':
		case 'reply':

		case 'copy': //
			// var author = entry.from_id ? 'http://vk.com/id' + entry.from_id : null;
			var author;
			if (entry.from_id) {
				if (entry.from_id > 0) {
					author = 'http://vk.com/id' + entry.from_id;
				} else {
					author = 'http://vk.com/club' + (entry.from_id * -1);
				}
			}

			return {
				'url': 'http://vk.com/wall' + entry.from_id + '_' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/cf6681b2f294c4a7a648ed2bf1ea0323',
				'author': author,
				'metrics': {
					'comments': entry.comments.count,
					'likes': entry.likes.count,
					'reposts': entry.reposts.count
				},
				'content': entry.text,
				'created_at': entry.date
			};

		case 'profile':
			return {
				'url': 'http://vk.com/id' + entry.uid,
				'entry-type': 'urn:fos:sync:entry-type/cf6681b2f294c4a7a648ed2bf1e9c2a8',
				'first-name': entry.first_name,
				'last-name': entry.last_name,
				'nickname': entry.nickname,
				'avatar': entry.photo,
				// 'created_at': (new Date (entry.created_at)).getTime () / 1000,
				'metrics': entry.counters
			};

		case 'group':
			return {
				'url': 'http://vk.com/club' + entry.gid,
				'entry-type': 'urn:fos:sync:entry-type/1f1d48152476612c3d5931cb927574a7',
				'title': entry.name,
				'nickname': entry.screen_name,
				'avatar': entry.photo
			};

		// group, page

		default:
			// if uid then user else
			console.log (entry);
			throw not_implemented (entry.post_type || type);
	}
}


var url = 'http://89.179.119.16:8001';


(new Slave ({
	title: 'vkontakte api',
	version: '0.0.1'
}))
	.use ('urn:fos:sync:feature/29e5fa0b4e79c2412525bcdc576a92a2', function resolveToken (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVkontakte (token),
			promise = Promises.promise ();
		
		vk ('getProfiles', {
			fields: 'uid, first_name, last_name, nickname, screen_name, sex, photo, birthdate, city, country, rate, counters'
		}, function (error, results) {
			if (error) {
				promise.reject (error);
				return null;
			}

			var entry = normalize (results [0], 'profile');

			entry.tokens = [token._id];

			Promises.when (emit (entry))
				.then (_.bind (promise.fulfill, promise))
				.fail (_.bind (promise.reject, promise))
				.done ();
		});

		return promise;
	})

	.use ('urn:fos:sync:feature/1f1d48152476612c3d5931cb9239fc2a', function searchAllNews (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVkontakte (token),
			promise = Promises.promise ();

		vk ('newsfeed.search', {q: task.keywords}, function (error, results) {
			if (error) {
				promise.reject (error);
				return;
			}

			Promises.all (
				_.map (results, function (data) {
					if (typeof data == 'object') {
						var entry = normalize (data);
						if (entry) {
							return emit (entry);
						}
						
					}
				})
			)
				.then (_.bind (promise.fulfill, promise))
				.fail (_.bind (promise.reject, promise))
				.done ();
		});

		return promise;
	})

	.use ('urn:fos:sync:feature/1f1d48152476612c3d5931cb924c4aa6', function explain (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVkontakte (token),
			promise = Promises.promise (),
			tmp;

		if (tmp = task.url.match (/\/id(\d+)/)) {
			var uid = tmp [1];

			vk ('getProfiles', {
				uids: uid,
				fields: 'uid, first_name, last_name, nickname, screen_name, sex, photo, birthdate, city, country, rate, counters'
			}, function (error, results) {
				if (error) {
					promise.reject (error);
					return null;
				}

				var entry = normalize (results [0], 'profile');

				Promises.when (emit (entry))
					.then (_.bind (promise.fulfill, promise))
					.fail (_.bind (promise.reject, promise))
					.done ();
			});
		} else if (tmp = task.url.match (/\/club(\d+)/)) {
			var gid = tmp [1];

			vk ('groups.getById', {
				gid: gid
			}, function (error, results) {
				if (error) {
					promise.reject (error);
					return null;
				}

				var entry = normalize (results [0], 'group');

				Promises.when (emit (entry))
					.then (_.bind (promise.fulfill, promise))
					.fail (_.bind (promise.reject, promise))
					.done ();
			});
		} else {
			promise.reject ('Unkown url ' + task.url);
		}
		

		return promise;
	})

	.fail (function (error) {
		console.error ('Error', error);

		var reconnect = _.bind (function () {
			this.connect (SocketIO, url)
		}, this);
		
		_.delay (reconnect, 1000);
	})

	.connect (SocketIO, url);

