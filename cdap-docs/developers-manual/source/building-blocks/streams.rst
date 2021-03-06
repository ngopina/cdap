.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _streams:

=======
Streams
=======

*Streams* are the primary means of bringing data from external systems into the CDAP in real time.
They are ordered, time-partitioned sequences of data, usable for real-time collection and consumption of data.

They can be created programmatically within your application, using the
:ref:`http-restful-api-stream`, the :ref:`stream-client` of the :ref:`client-api`, or by
using the :ref:`CDAP Command Line Interface. <cli>` 

Data written to a stream can be consumed in real time by :ref:`flows <flows>` or in batch
by :ref:`MapReduce programs. <mapreduce>`.


.. rubric:: Creating a Stream

You specify a stream in your :ref:`application <applications>` specification::

  addStream(new Stream("myStream"));

This specifies a new stream named *myStream*. 

Streams are uniquely identified by the ID string (a "name") and are explicitly created
before being used. Names used for streams need to be unique across the CDAP instance, as
streams are shared between applications.


.. rubric:: Writing To a Stream

You can write to streams either one operation at a time or in batches, using either the
:ref:`http-restful-api-stream` or the :ref:`Command Line Interface. <cli>`

Each individual signal sent to a stream is stored as a ``StreamEvent``, which is comprised
of a header (a map of strings for metadata) and a body (a blob of binary data).


.. rubric::  Reading From a Stream

To convert the binary data into a string, you need to take into account the character
encoding of the data, such as shown in this code fragment::

  @ProcessInput
  public void process(StreamEvent myStreamEvent) {
    String event = Charsets.UTF_8.decode(myStreamEvent.getBody()).toString();
  ...
  }


.. rubric:: Stream Time-To-Live (TTL)

Streams are persisted by CDAP, and once an event has been sent to a stream, by default it
never expires. The Time-To-Live (TTL) property governs how long an event is valid for
consumption since it was written to the stream. The default TTL for all streams is
infinite, meaning that events will never expire. The TTL property of a stream can be
changed, using the :ref:`http-restful-api-stream`, the :ref:`stream-client` of the
:ref:`client-api`, or by using the :ref:`Command Line Interface. <cli>`


.. rubric:: Truncating and Deleting a Stream

Streams can be truncated, which means deleting all events that were ever written to the
stream. This is permanent and cannot be undone. They can be truncated through the using
the :ref:`http-restful-api-stream`, the :ref:`stream-client` of the :ref:`client-api`, or
by using the :ref:`Command Line Interface <cli>`.

Deleting a stream means deleting the endpoint so that events can no longer be written to
it. This is permanent and cannot be undone. If another stream is created with the same
name, it will not return any of the previous stream's events.


.. _streams-notifications:

.. rubric:: Stream Notifications

Streams publish notifications internally to CDAP when they ingest data. The increment of data that they have to ingest
in order for a notification to be published is defined by the ``notification.threshold.mb`` configuration of a stream,
and can be changed using the :ref:`http-restful-api-stream`, the :ref:`stream-client` of the :ref:`client-api`, or
by using the :ref:`Command Line Interface <cli>`. When creating a stream, by default the threshold is set to the value of
``stream.notification.threshold`` in the :ref:`cdap-site.xml <appendix-cdap-site.xml>`.

The notifications describe the absolute size of events ever ingested by a stream, and as such, they will always describe
increasing data size. In particular, they do not reset when the stream is truncated, and they do not decrease when
part of the data ingested by the stream has reach the TTL.

Stream-size notifications are used by :ref:`stream-size schedules <stream-size-schedules>`.

.. rubric:: Examples of Using Streams

Streams are included in just about every CDAP :ref:`application <apps-and-packs>`,
:ref:`tutorial <tutorials>`, :ref:`guide <guides-index>` or :ref:`example <examples-index>`.

- The simplest example, :ref:`Hello World <examples-hello-world>`, demonstrates **using a
  stream to ingest** a name into a dataset.

- For an example of **pushing events to a stream from the Command Line,** see the :ref:`Purchase
  example <examples-purchase>`, and its script ``inject-data`` that injects data to a stream.

- For an example of **reading events from a stream,** see the 
  :ref:`Purchase example <examples-purchase>`, where the class ``PurchaseStreamReader``
  reads events from a stream. 

- For an example of **reading from a stream with a MapReduce program,** see the 
  :ref:`cdap-mapreduce-guide`, where the class ``TopClientsMapReduce`` uses the method
  ``StreamBatchReadable`` to read events from a stream.
