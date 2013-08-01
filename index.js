var Slave = require ('fos-sync-slave'),
	SocketIO = require ('socket.io-client'),
	vkontakte = require ('vkontakte'),
	_ = require ('lodash'),
	Promises = require ('vow'),
	LRU = require ('lru-cache'),
	API_RATE_WINDOW = (1000 * 0.5) + 1,
	request = require ('request'),
	cache = LRU ({
		max: 100
	});

_.rateLimit = function(func, rate, async) {
	var queue = [];
	var timeOutRef = false;
	var currentlyEmptyingQueue = false;

	var emptyQueue = function () {
		if (queue.length) {
			currentlyEmptyingQueue = true;

			_.delay (function() {
				if (async) {
					_.defer (function () {
						var f = queue.shift ();
						if (f) f.call ();
					});
				} else {
					var f = queue.shift ();
					if (f) f.call ();
				}

				emptyQueue ();
			}, rate);
		} else {
			currentlyEmptyingQueue = false;
		}
	};

	return function () {
		// get arguments into an array
		var args = _.map (arguments, function(e) {
			return e;
		});
		
		// call apply so that we can pass in arguments as parameters as opposed to an array
		queue.push (
			_.bind.apply (this, [func, this].concat (args))
		);

		if (!currentlyEmptyingQueue) {
			emptyQueue ();
		}
	};
};


var getVKontakte = function (token) {
	var key = JSON.stringify (token),
		api = cache.get (key);

	if (!api) {
		var limitedRequest = _.rateLimit (request, API_RATE_WINDOW, true);
		
		api = vkontakte (limitedRequest, token.access_token);
		setupRateLimits (api);
		cache.set (key, api);
	}

	return api;
};

function normalize (entry, type) {
	function not_implemented (what) {
		return new Error ('Not implemented ' + what);
	}

	if(entry.from_id) {
		var author;

		if (entry.from_id) {
			if (entry.from_id > 0) {
				author = 'http://vk.com/id' + entry.from_id;
			} else {
				author = 'http://vk.com/club' + (entry.from_id * -1);
			}
		}
	}

	switch (entry.post_type || type) {
		case 'post':
		case 'reply':

		case 'copy': //
		case 'wall':
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
				'avatar': entry.photo_50,
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

		case 'topic':
			return {
				'url': 'http://vk.com/topic' + (entry.group_id * -1) + '_' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/5ba5d4d5335fa863936b2a2c46e122cc',
				'author': entry.created_by,
				'title': entry.title,
				'nickname': entry.screen_name,
				'metrics': {
					'comments': entry.comments
				},
				'created_at': entry.created
			};

		case 'topic_post':
			return {
				'url': 'http://vk.com/topic' + (entry.group_id * -1) + '_' + entry.topic_id + '?post=' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/5ba5d4d5335fa863936b2a2c46eaec60',
				'author': author,
				'nickname': entry.screen_name,
				'created_at': entry.date,
				'metrics': {
					'likes': entry.likes.count
				}
			};

		case 'wall_post':
		
			return {
				'url': 'http://vk.com/wall' + entry.from_id + '_' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/dadd005a5055a5e9890112c44be64276',
				'author': author,
				'metrics': {
					'likes': entry.likes.count,
				},
				'content': entry.text,
				'created_at': entry.date
			};
	

		// group, page

		default:
			// if uid then user else
			console.log (entry);
			throw not_implemented (entry.post_type || type);
	}
}

function getComments(vk, emit, object) {
	var promise = Promises.promise ();
		params = {need_likes: 1, count: 100},
		method = '';

	switch (object.type)
	{
		case 'topic':
			method = 'board.getComments';
			params.group_id = object.group_id;
			params.topic_id = object.id;
			break;

		case 'wall':
			method = 'wall.getComments';
			params.owner_id = object.owner_id;
			params.post_id = object.id;
			break;

		default:
			// if uid then user else
			console.log (object);
			return new Error ('Not implemented ' + object.type);

	}

	vk(method, params, function (error, results) {
		if (error) {
				promise.reject (error);
				return;
			}

			Promises.all (
				_.map (results, function (data) {
					if (typeof data == 'object') {
						var entry = normalize (data, object.type + '_post');

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
}


var url = 'http://89.179.119.16:8001';


(new Slave ({
	title: 'vkontakte api',
	version: '0.0.1'
}))
	.use ('urn:fos:sync:feature/29e5fa0b4e79c2412525bcdc576a92a2', function resolveToken (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise ();
		
		vk ('users.get', {
			fields: 'nickname,screen_name,sex,bdate,city,country,timezone,photo_50,photo_100,photo_200_orig,has_mobile,contacts,education,online,counters,relation,last_seen,status,can_write_private_message,can_see_all_posts,can_post,universities,schools'
		}, function (error, results) {
			if (error) {
				promise.reject (error);
				return null;
			}

			var entry = normalize (results [0], 'profile');

			//entry.tokens = [token._id];

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
			vk = getVKontakte (token),
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

	.use ('urn:fos:sync:feature/e9b93bf34cc142491627dd19c99b9f44', function get_wall (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise (),
			group_id = task.url.match (/\/club(\d+)/) [1];

		vk ('wall.get', {owner_id: (group_id * -1), count: 10}, function (error, results) {
			if (error) {
				promise.reject (error);
				return;
			}

			Promises.all (
				_.map (results, function (data) {
					if (typeof data == 'object') {
						if (entry = normalize (data)) {
							emit (entry);
						}

						data.type = 'wall';
						data.owner_id = (group_id * -1);
						return getComments(vk, emit, data);
						
					}
				})
			)
				.then (_.bind (promise.fulfill, promise))
				.fail (_.bind (promise.reject, promise))
				.done ();
		});

		return promise;
	})

	.use ('urn:fos:sync:feature/c83d95fab6adfea73e3fe793de43ba90', function get_topics (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise (),
			group_id = task.url.match (/\/club(\d+)/) [1];

		vk ('board.getTopics', {owner_id: group_id, count: 100}, function (error, results) {
			if (error) {
				promise.reject (error);
				return;
			}

			Promises.all (
				_.map (results, function (data) {
					if (typeof data == 'object') {
						var entry = normalize (data, 'topic');

						if (entry) {
							entry.group_id = group_id;
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
			vk = getVKontakte (token),
			promise = Promises.promise (),
			tmp;

		if (tmp = task.url.match (/\/id(\d+)/)) {
			var user_id = tmp [1];

			vk ('users.get', {
				user_ids: user_id,
				fields: 'nickname,screen_name,sex,bdate,city,country,timezone,photo_50,photo_100,photo_200_orig,has_mobile,contacts,education,online,counters,relation,last_seen,status,can_write_private_message,can_see_all_posts,can_post,universities,schools'
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

