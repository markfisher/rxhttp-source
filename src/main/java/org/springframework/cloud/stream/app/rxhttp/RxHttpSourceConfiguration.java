/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.rxhttp;

import static org.springframework.web.bind.annotation.RequestMethod.POST;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;

import reactor.core.publisher.Flux;

/**
 * @author Mark Fisher
 */
@Controller
@EnableBinding(Source.class)
public class RxHttpSourceConfiguration {

	@Autowired
	private Source channels;

	@RequestMapping(path = "${http.pathPattern:/}", method = POST, consumes = {"text/*", "application/json"})
	@ResponseStatus(HttpStatus.ACCEPTED)
	public void handleStrings(@RequestBody Flux<String> body, @RequestHeader(HttpHeaders.CONTENT_TYPE) Object contentType) {
		body.log().subscribe(s -> sendMessage(s, contentType));
	}

	@RequestMapping(path = "${http.pathPattern:/}", method = POST, consumes = "*/*")
	@ResponseStatus(HttpStatus.ACCEPTED)
	public void handleBytes(@RequestBody Flux<byte[]> body, @RequestHeader(HttpHeaders.CONTENT_TYPE) Object contentType) {
		body.log().subscribe(b -> sendMessage(b, contentType));
	}

	private void sendMessage(Object body, Object contentType) {
		this.channels.output().send(MessageBuilder.createMessage(body,
				new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, contentType))));
	}
}
