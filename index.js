var Slave = require ('fos-sync-slave'),
	SocketIO = require ('socket.io-client'),
	vkontakte = require ('vkontakte'),
	_ = require ('lodash'),
	Promises = require ('vow'),
	LRU = require ('lru-cache'),
	API_RATE_WINDOW = (1000 * 0.5) + 1,
	request = require ('request'),
	Url = require ('url'),
	moment = require('moment'),
	cache = LRU ({
		max: 1000
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

function iterate (vk, method, params, filter, callback) {
	var promise = Promises.promise(),
		promises = [];

	params.offset = params.offset || 0;
	params.limit = params.limit || 100;


	function finish () {
		Promises.all (promises)
			.always (_.bind (promise.fulfill, promise))
			.done ();
	}


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
				finish ();
				return;
			}

			for (var i = 0, row; i < rows.length; i++) {
				row = rows [i];
				if (filter (row)) {
					promises.push (
						callback (null, row)
					);
				} else {
					finish ();
					stop = true;
					break;
				}
			}

			if (rows.length < params.limit) {
				finish ();
				return;
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

function getGroupContacts (vk, group_id, callback) {
	//TODO: add caching
	return getGroupInfo (vk, group_id, function (error, result) {
		if (error) {
			callback (error);
			return null;
		}

		if(!result || !result.contacts) {
			callback ('getGroupContacts return null for ' + group_id);
			return null;
		}

		if (!result.contacts.length) {
			callback ('Group has not contacts');
			return null;	
		}

		callback (error, result.contacts);
	});
};

function getUserInfo (vk, user_id, callback) {
	//TODO: add caching
	var params = {
		fields: 'nickname,screen_name,sex,bdate,city,country,timezone,photo_50,photo_100,photo_200_orig,has_mobile,contacts,education,online,counters,relation,last_seen,status,can_write_private_message,can_see_all_posts,can_post,universities,schools'
	};

	if (user_id) {
		params.user_ids = user_id;
	}

	return vk ('users.get', params, function (error, result) {
		if(error) {
			callback(error);
			return null;
		} else {
			callback(null, result [0]);
		}
	});
};

function getCityById (vk, city_id, callback) {
	return vk ('database.getCitiesById', {city_ids: city_id}, function (error, result) {
		if(error) {
			callback(error);
			return null;
		} else {
			callback(null, result [0]);
		}
	});
};


function getGroupInfo (vk, group_id, callback) {
	//TODO: add caching
	return vk ('groups.getById', {
		group_id: group_id,
	fields: 'city,country,place,description,wiki_page,members_count,counters,start_date,end_date,can_post,can_see_all_posts,activity,status,contacts,links,fixed_post,verified,site'
	}, function (error, result) {
		if(error) {
			callback(error);
			return null;
		} else {
			callback(null, result [0]);
		}
	});
};

function getPM (vk, out, filter, callback) {
	return iterate (vk, 'messages.get', {out: out, count: 100}, filter, callback);	
};

function urlResolve (vk, url) {
	if (url.match(/vk.com\/id(\d+)$/) || url.match(/vk.com\/club(\d+)$/)) {
		return Promises.fulfill (url);
	} else if (tmp = url.match(/vk.com\/(public|event)(\d+)$/)) {
		url = 'http://vk.com/club' + tmp [2];
		return Promises.fulfill (url);
	} else if (tmp = url.match(/vk.com\/(.+)/)) {
		var promise = Promises.promise();

		getGroupInfo (vk, tmp [1], function (error, result) {
			if (error) {
				getUserInfo (vk, tmp [1], function (error, result) {
					if (error) {
						// todo: throw error
					} else {
						url = 'http://vk.com/id' + tmp [1];
					}
					promise.fulfill (url);
				});
			} else {
				url = 'http://vk.com/club' + tmp [1];
				promise.fulfill (url);
			}
		});

		return promise;
	}
};

function resolveAttachments (entry) {
	if (entry.attachments) {
		entry.attached = {
			photos: [],
			video: [],
			audio: [],
			doc: []
		};

		for (var i in entry.attachments) {
			var attachment = entry.attachments [i];

			switch (attachment.type) {
				case 'photo':
					entry.attached.photos.push(attachment.photo.photo_604);
					break;

				case 'video':
					entry.attached.video.push('http://vk.com/video' + attachment.video.owner_id + '_' + attachment.video.id);
					break;


				case 'audio':
					entry.attached.audio.push(attachment.audio.url);
					break;


				case 'doc':
					entry.attached.doc.push(attachment.doc.url);
					break;

				/*
				case 'wall':
					entry.attached.wall.push(attachment.wall.);
					break;


				case 'wall_reply':
					entry.attached.wall_reply.push(attachment.wall_reply.);
					break;
					*/
			}
		}
	}

	return entry;
};

function normalize (entry, type, vk) {
	var data = preNormalize (entry, type);

	if(type == 'profile' && data.city) {
		var promise = new Promises.promise();

		getCityById (vk, data.city, function (error, result) {
			if (error) {
				promise.reject (error);
				return null;
			}

			if (!result) {
				promise.reject ('getCityById return null for ' + data.city);
				return;
			}

			data.city = result.title;

			promise.fulfill (data);
		});

		return promise;
	}

	if (data.author && (group_id = (data.author).match (/http:\/\/vk.com\/club(\d+)/))) {
		var promise = new Promises.promise();

		getGroupContacts (vk, group_id [1], function (error, results) {
			if (error) {
				promise.reject (error);
				return null;
			}

			var contact = null;

			for (i in results) {
				if (results[i].user_id) {
					contact = results[i];
				}
			};

			if(!contact) {
				promise.reject ('Message was written by a group with no VK contacts');
				return null;
			}		

			data.author = 'http://vk.com/id' + contact.user_id;

			promise.fulfill (data);
		});

		return promise;
	}
	
	return data;
};




function preNormalize (entry, type) {
	var promise = Promises.promise();

	function not_implemented (what) {
		return new Error ('Not implemented ' + what);
	}

	if (entry.attachments) {
		entry = resolveAttachments (entry);
	}

	var author;

	if(entry.from_id) {
		if (entry.from_id > 0) {
			author = 'http://vk.com/id' + entry.from_id;
		} else {
			author = 'http://vk.com/club' + (entry.from_id * -1);
		}
	} else if (entry.created_by) {
		if (entry.created_by > 0) {
			author = 'http://vk.com/id' + entry.created_by;
		} else {
			author = 'http://vk.com/club' + (entry.created_by * -1);
		}	
	} else if (entry.author) {
		author = entry.author;
	}

	switch (type || entry.post_type) {
		
		case 'post':
		case 'reply':

		case 'copy':
			return {
				'url': 'http://vk.com/wall' + entry.from_id + '_' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/dadd005a5055a5e9890112c44be64276',
				'author': author,
				'ancestor': 'http://vk.com/wall' + entry.from_id,
				'metrics': {
					'comments': entry.comments.count,
					'likes': entry.likes.count,
					'reposts': entry.reposts.count
				},
				'content': entry.text,
				'created_at': entry.date,
				'attached': entry.attached ? entry.attached : null
			};

		case 'profile':
			var avatar = entry.photo_50;
				//birthdate = moment(entry.bdate, ['DD.MM.YYYY', 'D.MM.YYYY', 'DD.M.YYYY', 'D.M.YYYY']);

			if (avatar == 'http://vk.com/images/deactivated_c.gif' || avatar == 'https://vk.com/images/camera_c.gif') {
				avatar = null;
			}

			return {
				'url': 'http://vk.com/id' + entry.id,
				'entry-type': 'urn:fos:sync:entry-type/cf6681b2f294c4a7a648ed2bf1e9c2a8',
				'first-name': entry.first_name,
				'family-name': entry.last_name,
				'nickname': entry.nickname,
				'avatar': avatar,
				'metrics': entry.counters,
				'birth-date': entry.bdate,//birthdate.isValid() ? birthdate.unix() * 1000 : null,
				'phone': entry.has_mobile ? entry.mobile_phone : null,
				'city': entry.city
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
				'author': author,
				'title': entry.title,
				'metrics': {
					'comments': entry.comments ? entry.comments : null
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
					'likes': entry.likes ? entry.likes.count : null
				},
				'content': entry.text,
				'attached': entry.attached ? entry.attached : null
			};

		case 'wall': 
			return {
				'url': 'http://vk.com/wall' + entry.owner_id,
				'entry-type': 'urn:fos:sync:entry-type/e242b98044c627d2009df1ad9267cff2',
				'title': 'Стена'
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
				'created_at': entry.date,
				'attached': entry.attached ? entry.attached : null
			};

		case 'message':
			return {
				'url': 'http://vk.com/mail?act=show&id=' + entry.id + '&owner_id=' + entry.owner_id,
				'entry-type': 'urn:fos:sync:entry-type/cf6681b2f294c4a7a648ed2bf1ea7f50',
				'author': author,
				'title': entry.title ? entry.title : '',
				'content': entry.body,
				'created_at': entry.date,
				'attached': entry.attached ? entry.attached : null
			};
	

		// group, page

		default:
			// if uid then user else
			console.log (entry);
			throw not_implemented (entry.post_type || type);
	}
};

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
			return Promises.reject (error);
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

		var entry = normalize (row, type, vk);

		if (entry) {
			return emit (entry);
		}
	});
};


function getTopics (vk, group_id, topic_id, filter, callback) {
	return iterate (vk, 'board.getTopics', {group_id: group_id, topic_ids: topic_id, order: 2}, filter, callback);
};


var url = 'http://siab.frossa.ru:8001';

function restart () {
	_.delay (function () {
		process.exit ();
	}, 2500);
};


(new Slave ({
	title: 'vkontakte api',
	version: '0.0.1'
}, {
	restart: restart
}))
	.use ('urn:fos:sync:feature/29e5fa0b4e79c2412525bcdc576a92a2', function resolveToken (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise ();
		
		getUserInfo (vk, null, function (error, result) {
			if (error) {
				promise.reject (error);
				return null;
			}

			if (result.deactivated) {
				promise.reject ('User deactivated');
				return null;	
			}

			var entry = normalize (result, 'profile', vk);

			entry.tokens = [token._id];

			Promises.when (emit (entry))
				.then (_.bind (promise.fulfill, promise))
				.fail (_.bind (promise.reject, promise))
				.done ();
		});

		return promise;
	})

	.use ('urn:fos:sync:feature/e9b93bf34cc142491627dd19c99b9f44', function getWall (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise (),
			owner_id;

		return urlResolve (vk, task.url)
			.then (function (task_url) {
				if (tmp = task_url.match (/\/id(\d+)/)) {
					owner_id = tmp [1];
				} else if (tmp = task_url.match (/\/club(\d+)/)) {
					owner_id = (tmp [1] * -1);
				} else if (tmp = task_url.match (/\/wall(|\-)(\d+)/)) {
					owner_id = tmp [1] + tmp [2];
				}

				return iterate (vk, 'wall.get', {owner_id: owner_id}, function (row) {
					return Boolean((row.date * 1000) >= task ['scrape-start']);
				}, function (error, row) {
					if (error) {
						promise.reject (error);
						return;
					}

					if (typeof row == 'object') {
						row.owner_id = owner_id;
						row.post_id = row.id;

						if (entry = normalize (row, 'wall_post', vk)) {
							emit (entry);
						}

						return getComments (vk, emit, row, 'wall_post')
							.fail (function (error) {
								emit (new Error (error.error_msg));
							});
					}
				});
			});
	})

	.use ('urn:fos:sync:feature/c83d95fab6adfea73e3fe793de43ba90', function getTopicPosts (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise (),
			tmp, group_id, post_id;

		return urlResolve (vk, task.url)
			.then (function (task_url) {
				if (tmp = task_url.match (/\/club(\d+)/)) {
					group_id = tmp [1];
				} else if (tmp = task_url.match (/\/topic-(\d+)/)) {
					group_id = tmp [1];
				}

				if (tmp = task_url.match (/\/topic-(\d+)_(\d+)/)) {
					post_id = tmp [2];
				}

				return getTopics(vk, group_id, post_id, function (row) {
					return Boolean((row.created * 1000) >= task['scrape-start']);
				}, function (error, row) {
					if (error) {
						return Promises.reject (error);
					}

					if (typeof row == 'object') {
						row.type = 'topic';
						row.group_id = group_id;

						if (entry = normalize (row, 'topic', vk)) {
							emit (entry);

							Promises.when (emit (entry))
								.then (_.bind (promise.fulfill, promise))
								.fail (_.bind (promise.reject, promise))
								.done ();
						}

						return getComments(vk, emit, row, 'topic_post');
					}
				});
			});
	})

	.use ('urn:fos:sync:feature/04c8d61b0ab10abd2b425c7cf6fea33a', function getPersonalMessages (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise ();

		var filter = function (row) {
			return Boolean((row.date * 1000) >= task['scrape-start']);
		};

		var callback = function (error, row) {
			if (error) {
				promise.reject (error);
				return;
			}

			if (typeof row == 'object') {
				row.owner_id = task.url.match (/\/id(\d+)/) [1];
				row.author = 'http://vk.com/id' + row.user_id;

				if (entry = normalize (row, 'message', vk)) {

					Promises.when (emit (entry))
						.then (_.bind (promise.fulfill, promise))
						.fail (_.bind (promise.reject, promise))
						.done ();
				}
			}
		};

		return Promises.all ([
			getPM (vk, 1, filter, callback),
			getPM (vk, 0, filter, callback)
		]);
	})

	.use ('urn:fos:sync:feature/e242b98044c627d2009df1ad92775771', function reply (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise (),
			_self = this,
			reply_to_id = task['reply-to'].match (/\/id(\d+)/) [1],
			reply_to_name,
			tmp, type;


		return urlResolve (vk, task.url)
			.then (function (task_url) {
				getUserInfo (vk, reply_to_id, function (error, result) {
					if (error) {
						reply_to_name = '...';
					} else {
						reply_to_name = result.first_name;
					}

					if (tmp = task_url.match (/\/wall(|\-)(\d+)\_(\d+)\?reply=(\d+)/)) {
						var owner_id = tmp [1] + tmp [2],
							post_id = tmp [3],
							reply_id = tmp [4],	
							type = 'wall_post';
					} else if (tmp = task_url.match (/\/wall(|\-)(\d+)\_(\d+)/)) {
						var owner_id = tmp [1] + tmp [2],
							post_id = tmp [3],
							type = 'wall_post';
					} else if (tmp = task_url.match (/\/topic(|\-)(\d+)\_(\d+)\?post=(\d+)/)) {
						var group_id = tmp [2],
							topic_id = tmp [3],
							post_id = tmp [4],	
							type = 'topic_post';
					} else if (tmp = task_url.match (/\/topic(|\-)(\d+)\_(\d+)/)) {
						var group_id = tmp [2],
							topic_id = tmp [3],
							type = 'topic_post';
					} else if (tmp = task_url.match (/\/id(\d+)/)) {
						var user_id = tmp [1],
							type = 'message';
					}

					var callback = function (error, result) {
						if (error) {
							promise.reject (error);
							return null;
						}

						var reply = {
							author: task.author,
							text: task.content,
							date: parseInt(Date.now() / 1000)
						};

						if (type == 'wall_post') {
							reply.owner_id = owner_id;
							reply.post_id = post_id;
							reply.reply_id = result.comment_id;
						} else if (type == 'topic_post') {
							reply.group_id = group_id;
							reply.topic_id = topic_id;
							reply.id = result;
						} else if (type == 'message') {
							reply.owner_id = task.author.match (/\/id(\d+)/) [1]
							reply.body = task.content;
							reply.id = result;
						};

						var entry = normalize (reply, type, vk);
						entry.issue = task.issue;

						Promises.when (emit (entry))
							.then (_.bind (promise.fulfill, promise))
							.fail (_.bind (promise.reject, promise))
							.done ();
					};

					if (type == 'wall_post') {
						if (reply_id) task.content = reply_to_name + ', ' + task.content;
						vk ('wall.addComment', {
							owner_id: owner_id,
							post_id: post_id,
							text: task.content,
							reply_to_comment: reply_id ? reply_id : ''
						}, callback);
					} else if (type == 'topic_post') {
						if (post_id) task.content = '[post' + post_id + '|' + reply_to_name + '], ' + task.content;

						vk ('board.addComment', {
							group_id: group_id,
							topic_id: topic_id,
							text: task.content
						}, callback);
					} else if (type == 'message') {
						vk ('messages.send', {
							user_ids: user_id,
							message: task.content
						}, callback);
					}

				});
			});
		
		

		return promise;
	})

	.use ('urn:fos:sync:feature/1f1d48152476612c3d5931cb9239fc2a', function searchAllNews (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			query = Url.parse (task.url, true).query,
			keywords = query.q || query ['c[q]'];

			//console.log(keywords);

/*
		if (/feed\?q=.*$/.test (url) || /search\?c\[q\]=(.+)\&c\[section\]=statuses/.test (url)) {

		}
	*/

		var filter = function (row) {
			return Boolean((row.date * 1000) >= task['scrape-start']);
		};

		var callback = function (error, row) {
			if (error) {
				promise.reject (error);
				return;
			}

			if (typeof row == 'object') {
				
				if (entry = normalize (row, null, vk)) {
					console.log (entry);
					return emit (entry);
				}

				//return getComments(vk, emit, row);
			}
		};

		var params = {
			q: keywords,
			start_time: task['scrape-start']
		};

		return iterate (vk, 'newsfeed.search', params, filter, callback);
	})

	.use ('urn:fos:sync:feature/1f1d48152476612c3d5931cb924c4aa6', function explain (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise (),
			tmp;

		
		return urlResolve (vk, task.url)
			.then (function (task_url) {
				//explain user profile
				if (tmp = task_url.match (/\/id(\d+)/)) {
					var user_id = tmp [1];

					getUserInfo (vk, user_id, function (error, result) {
						if (error) {
							promise.reject (error);
							return null;
						}

						if (result.deactivated) {
							promise.reject ('User deactivated');
							return null;	
						}

						var entry = normalize (result, 'profile', vk);

						if (typeof emit != 'function') {
							console.log ('?', emit);
						}

						Promises.when (emit (entry))
							.then (_.bind (promise.fulfill, promise))
							.fail (_.bind (promise.reject, promise))
							.done ();
					});

				// explain group
				} else if (tmp = task_url.match (/\/club(\d+)/)) {
					var group_id = tmp [1];

					getGroupInfo (vk, group_id, function (error, result) {
						if (error) {
							promise.reject (error);
							return null;
						}

						var entry = normalize (result, 'group', vk);

						Promises.when (emit (entry))
							.then (_.bind (promise.fulfill, promise))
							.fail (_.bind (promise.reject, promise))
							.done ();
					});

				// explain wall post
				} else if (tmp = task_url.match (/\/wall(|\-)(\d+)\_(\d+)/)) {
					vk ('wall.getById', {
						posts: tmp [1] + tmp [2] + '_' + tmp [3]
					}, function (error, results) {
						if (error) {
							promise.reject (error);
							return null;
						}

						object = results [0];
						object.owner_id = tmp [1] + tmp [2];
						object.post_id = tmp [3];

						var entry = normalize (object, 'wall_post', vk);

						Promises.when (emit (entry))
							.then (_.bind (promise.fulfill, promise))
							.fail (_.bind (promise.reject, promise))
							.done ();
					});

				// explain wall
				} else if (tmp = task_url.match (/\/wall(|\-)(\d+)/)) {
					var object = {};

					object.owner_id = tmp [1] + tmp [2];

					var entry = normalize (object, 'wall');

					return emit(entry);

				// explain topic post
				} else if (tmp = task_url.match (/\/topic\-(\d+)\_(\d+)/)) {
					var group_id = tmp [1],
						post_id = tmp [2];

					getTopics(vk, group_id, post_id, function (row) {
						return true;
					}, function (error, row) {
						if (error) {
							promise.reject (error);
							return;
						}

						row.type = 'topic';
						row.group_id = group_id;

						var entry = normalize (row, 'topic', vk);

						Promises.when (emit (entry))
							.then (_.bind (promise.fulfill, promise))
							.fail (_.bind (promise.reject, promise))
							.done ();
					});
				} else {
					promise.reject ('Unkown url ' + task.url);
				}
			});
	})

	.use ('urn:fos:sync:feature/01bdf56a3837bfd6afa8bf69b54f70e3', function getMentions (task) {
		var token = task._prefetch.token,
			emit = this.emitter (task),
			vk = getVKontakte (token),
			promise = Promises.promise (),
			user_id = task.url.match(/vk.com\/id(\d+)$/) [1];
		
		return iterate (vk, 'newsfeed.getMentions', {uid: user_id}, function (row) {
			return Boolean((row.date * 1000) >= task ['scrape-start']);
		}, function (error, row) {
			if (error) {
				Promises.reject (error);
				return;
			}

			if (typeof row == 'object') {
				if (entry = normalize (row, null, vk)) {
					Promises.when (emit (entry))
						.then (_.bind (promise.fulfill, promise))
						.fail (_.bind (promise.reject, promise))
						.done ();
				}
			}
		});
	})

	.fail (function (error) {
		console.error ('Error', error);

		var reconnect = _.bind (function () {
			this.connect (SocketIO, url)
		}, this);
		
		_.delay (reconnect, 1000);
	})

	.connect (SocketIO, url);


