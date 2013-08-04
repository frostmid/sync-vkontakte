var Slave = require ('fos-sync-slave'),
	SocketIO = require ('socket.io-client'),
	vkontakte = require ('vkontakte'),
	_ = require ('lodash'),
	Promises = require ('vow'),
	LRU = require ('lru-cache'),
	API_RATE_WINDOW = (1000 * 0.5) + 1,
	request = require ('request'),
	Url = require ('url'),
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

	switch (type || entry.post_type) {
		
		case 'post':
		case 'reply':

		case 'copy':
			return {
				'url': 'http://vk.com/wall' + entry.from_id + '_' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/dadd005a5055a5e9890112c44be64276',
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
			var avatar = entry.photo_50;

			if (avatar == 'http://vk.com/images/deactivated_c.gif' || avatar == 'https://vk.com/images/camera_c.gif') {
				avatar = null;
			}

			return {
				'url': 'http://vk.com/id' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/cf6681b2f294c4a7a648ed2bf1e9c2a8',
				'first-name': entry.first_name,
				'last-name': entry.last_name,
				'nickname': entry.nickname,
				'avatar': avatar,
				// 'created_at': (new Date (entry.created_at)).getTime () / 1000,
				'metrics': entry.counters
			};

		case 'group':
			return {
				'url': 'http://vk.com/club' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/1f1d48152476612c3d5931cb927574a7',
				'title': entry.name,
				'nickname': entry.screen_name,
				'avatar': entry.photo_50
			};

		case 'topic':
			return {
				'url': 'http://vk.com/topic' + (entry.group_id * -1) + '_' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/5ba5d4d5335fa863936b2a2c46e122cc',
				'author': entry.created_by,
				'title': entry.title,
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
				'ancestor': 'http://vk.com/topic' + (entry.group_id * -1) + '_' + entry.topic_id,
				'created_at': entry.date,
				'metrics': {
					'likes': entry.likes.count
				},
				'content': entry.text
			};

		case 'wall_post':
			return {
				'url': 'http://vk.com/wall' + entry.owner_id + '_' + entry.post_id + (entry.reply_id ? '?reply=' + entry.reply_id : ''),
				'entry-type': 'urn:fos:sync:entry-type/dadd005a5055a5e9890112c44be64276',
				'ancestor': 'http://vk.com/wall' + entry.owner_id + (entry.reply_id ? '_' + entry.post_id : ''),
				'author': author,
				'metrics': {
					'likes': entry.likes ? entry.likes.count : null,
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

function getComments(vk, emit, object, type) {
	var params = {need_likes: 1, count: 100},
		method = '';

	switch (type)
	{
		case 'topic_post':
			method = 'board.getComments';
			params.group_id = object.group_id;
			params.topic_id = object.id;
			break;

		case 'wall_post':
			method = 'wall.getComments';
			params.owner_id = object.owner_id;
			params.post_id = object.id;
			break;
 
		default:
			// if uid then user else
			console.log (object);
			return new Error ('Not implemented ' + type);

	}

	return iterate (vk, method, params, function (row) {
		return true;
	}, function (error, row) {
		if (error) {
			promise.reject (error);
			return;
		}

		switch (type) {
			case 'topic_post':
				row.group_id = params.group_id;
				row.topic_id = params.topic_id;
				break;

			case 'wall_post':
				row.post_id = params.post_id;
				row.owner_id = params.owner_id;
				row.reply_id = row.id;
				break;
		}

		var entry = normalize (row, type);

		if (entry) {
			return emit (entry);
		}
	});
}

function iterate (vk, method, params, filter, callback) {
	var promise = Promises.promise();

	params.offset = params.offset || 0;
	params.limit = params.limit || 100;

	function fetch () {
		vk (method, params, function (error, response) {
			if (error) {
				promise.reject(error)
				//callback (error);
				return;
			};

			var stop = false,
				rows = response.items,
				total = response.count;

			if (rows.length == 0) {
				promise.fulfill();
				return;
			}

			for (var i = 0, row; i < rows.length; i++) {
				row = rows [i];
				if (filter (row)) {
					callback (null, row);
				} else {
					promise.fulfill();
					stop = true;
					break;
				}
			}

			if (!stop) {
				params.offset += params.limit;

				fetch ();
			}
		});
	}

	fetch ();

	return promise;
}


function get_user (task) {
	var token = task._prefetch.token,
		emit = this.emitter (task),
		vk = getVKontakte (token),
		promise = Promises.promise (),
		user_id = task.url.match (/\/id(\d+)/) [1];

	vk ('users.get', {
		user_ids: user_id,
		fields: 'nickname,screen_name,sex,bdate,city,country,timezone,photo_50,photo_100,photo_200_orig,has_mobile,contacts,education,online,counters,relation,last_seen,status,can_write_private_message,can_see_all_posts,can_post,universities,schools'
	}, function (error, results) {
		if (error) {
			promise.reject (error);
			return null;
		}

		var response = results [0];

		if (response.deactivated) {
			promise.reject ('User deactivated');
			return null;	
		}

		var entry = normalize (response, 'profile');

		Promises.when (emit (entry))
			.then (_.bind (promise.fulfill, promise))
			.fail (_.bind (promise.reject, promise))
			.done ();
	});

	return promise;
};

function get_group (task) {
	var token = task._prefetch.token,
		emit = this.emitter (task),
		vk = getVKontakte (token),
		promise = Promises.promise (),
		group_id = task.url.match (/\/club(\d+)/) [1];

	vk ('groups.getById', {
		group_id: group_id,
		fields: 'city,country,place,description,wiki_page,members_count,counters,start_date,end_date,can_post,can_see_all_posts,activity,status,contacts,links,fixed_post,verified,site'
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

	return promise;
};

function get_wall (task) {
	var token = task._prefetch.token,
		emit = this.emitter (task),
		vk = getVKontakte (token);

	if (tmp = task.url.match (/\/id(\d+)/)) {
		owner_id = tmp [1];
	} else if (tmp = task.url.match (/\/club(\d+)/)) {
		owner_id = (tmp [1] * -1);
	} else if (tmp = task.url.match (/\/wall(|\-)(\d+)/)) {
		owner_id = tmp [1] + tmp [2];
	}

	return iterate (vk, 'wall.get', {owner_id: owner_id}, function (row) {
		return true;
	}, function (error, row) {
		if (error) {
			promise.reject (error);
			return;
		}

		if (typeof row == 'object') {
			row.owner_id = owner_id;
			row.post_id = row.id;

			if (entry = normalize (row, 'wall_post')) {
				emit (entry);
			}

			return getComments(vk, emit, row, 'wall_post');
		}
	});
};

function get_topics (task) {
	var token = task._prefetch.token,
		emit = this.emitter (task),
		vk = getVKontakte (token);

	if (tmp = task.url.match (/\/club(\d+)/)) {
		group_id = tmp [1];
	} else if (tmp = task.url.match (/\/topic-(\d+)/)) {
		group_id = tmp [1];
	}

	return iterate (vk, 'board.getTopics', {group_id: group_id}, function (row) {
		return true;
	}, function (error, row) {
		if (error) {
			promise.reject (error);
			return;
		}

		if (typeof row == 'object') {
			row.type = 'topic';
			row.group_id = group_id;

			if (entry = normalize (row, 'topic')) {
				emit (entry);
			}

			return getComments(vk, emit, row, 'topic_post');
		}
	});
};

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
			query = Url.parse (task.url, true).query,
			keywords = query.q || query ['c[q]'];

			console.log(keywords);

		return iterate (vk, 'newsfeed.search', {q: keywords, count: 10}, function (row) {
			return true;
		}, function (error, row) {
			if (error) {
				promise.reject (error);
				return;
			}

			if (typeof row == 'object') {
				
				if (entry = normalize (row)) {
					return emit (entry);
				}

				//return getComments(vk, emit, row);
			}
		});
	})

	.use ('urn:fos:sync:feature/e9b93bf34cc142491627dd19c99b9f44', get_wall)

	.use ('urn:fos:sync:feature/c83d95fab6adfea73e3fe793de43ba90', get_topics)

	.use ('urn:fos:sync:feature/1f1d48152476612c3d5931cb924c4aa6', function explain (task) {
		var promise = Promises.promise ();

		if (task.url.match (/\/id(\d+)/)) {
			return get_user.call(this, task);
		} else if (task.url.match (/\/club(\d+)/)) {
			return get_group.call(this, task);
		} else if (tmp = task.url.match (/\/wall(|\-)(\d+)\_(\d+)/)) {
			var token = task._prefetch.token,
				emit = this.emitter (task),
				vk = getVKontakte (token);

			return getComments.call(this, task, {owner_id: tmp [1] + tmp [2], post_id: tmp [3]}, 'wall_post');
		} else if (task.url.match (/\/wall(|\-)(\d+)/)) {
			return get_wall.call (this, task);
		} else if (tmp = task.url.match (/\/topic\-(\d+)\_(\d+)/)) {
			var token = task._prefetch.token,
				emit = this.emitter (task),
				vk = getVKontakte (token);

			return getComments.call(this, task, {group_id: tmp [1], topic_id: tmp [2]}, 'topic_post');
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

