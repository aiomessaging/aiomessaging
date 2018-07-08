Abstract
========

aiomessaging is a framework for massive message delivery to your application users.

The main goal to unify components interfaces and provide best aiomessaging
practices out of the box. The application developer can concentrate on business logic
instead of connection management, throttling, outbound rate limiting, retries etc.

There are a bunch of integrations on the way which helps to not think about
individual outbound service implementation too.

Main features:
- delivery guarantee
- delivery status monitoring
- advanced outbound routing based on user preferences
- message aggregation/digest

The main concept of aiomessaging is to receive events from your application,
generate individual events from them and route them through based on user
preferences (they are must be pr delivery pipeline provided by generator) and
previous delivery statuses.

*Input*

The input concept is about receiving events from your application. It may be
RabbitMQ queue, rest api o other application interface. RabbitMQ input queue
supported out of the box (all other input plugins use it as their ends).

*Output*

The main goal of output is to delivery generated message to the end-user.
Examples of output may be smtp, Amazon SES, WebSocket, rest-api or push service.

*Pipeline*

Internal aiomessaging pipeline receives events from inputs, generate batch of
messages from them and route them to one or more output backends. Pipeline
consists of the following parts:

* event filters — receives events on input and make decision to drop it or delay (for example to mutual exclusion of opposing events)
* message generators - receives events on input and generate a batch of messages based on business rules
* message filters - same as event filters but receives messages on its input
* message router - choosing next backend to send message through based on user preferences (provided by generator) and previous delivery backend statuses.
* delivery backend - delivery message to output (email, push etc.) and check status

