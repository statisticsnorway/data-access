import no.ssb.dapla.data.access.DataAccessApplicationBuilder;

module no.ssb.dapla.data.access {
    requires no.ssb.dapla.data.access.protobuf;
    requires no.ssb.dapla.auth.dataset.protobuf;
    requires no.ssb.dapla.catalog.protobuf;
    requires no.ssb.helidon.media.protobuf.json.server;
    requires org.slf4j;
    requires jul.to.slf4j;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.core;
    requires io.helidon.webserver;
    requires io.helidon.webserver.accesslog;
    requires io.helidon.config;
    requires java.net.http;
    requires io.helidon.common.reactive;
    requires logback.classic;
    requires io.helidon.metrics;
    requires io.helidon.health;
    requires io.helidon.health.checks;

    requires java.logging;
    requires io.helidon.webclient;

    requires com.google.gson; // required by JsonFormat in protobuf-java-util for serialization and deserialization

    requires com.google.protobuf.util;
    requires com.google.common;
    requires no.ssb.helidon.application;

    /*
     * Not so well documented requirements are declared here to force fail-fast with proper error message if
     * missing from jvm.
     */
    requires jdk.unsupported; // required by netty to allow reliable low-level API access to direct-buffers
    requires jdk.naming.dns; // required by netty dns libraries used by reactive postgres
    requires java.sql; // required by flyway
    requires io.helidon.microprofile.config; // metrics uses provider org.eclipse.microprofile.config.spi.ConfigProviderResolver

    requires io.helidon.tracing;
    requires com.google.auth.oauth2;
    requires com.google.auth;

    requires java.jwt;
    requires no.ssb.dapla.dataset.api;
    requires com.google.protobuf;

    provides no.ssb.helidon.application.HelidonApplicationBuilder with DataAccessApplicationBuilder;
}