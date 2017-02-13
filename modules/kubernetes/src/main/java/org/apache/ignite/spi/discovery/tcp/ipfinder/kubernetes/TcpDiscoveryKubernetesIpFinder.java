/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Kubernetes Service based IP finder.
 */
public class TcpDiscoveryKubernetesIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Init routine guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init routine latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Trust manager. */
    private TrustManager[] trustAll = new TrustManager[] {
        new X509TrustManager() {
            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            public void checkClientTrusted(X509Certificate[] certs, String authType) {}
            public X509Certificate[] getAcceptedIssuers() { return null; }
        }
    };

    /** Host verifier. */
    private HostnameVerifier trustAllHosts = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    /** Ignite's Kubernetes Service name. */
    private String serviceName = "ignite";

    /** Ignite Pod namespace name. */
    private String namespace = "default";

    /** Kubernetes master URL. */
    private String master = "https://kubernetes.default.svc.cluster.local:443";

    /** Account token location. */
    private String accountToken = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /** Kubernets API URL. */
    private URL url;

    /** SSL context */
    private SSLContext ctx;

    /**
     *
     */
    public TcpDiscoveryKubernetesIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        Collection<InetSocketAddress> addrs = new ArrayList<>();

        try {
            System.out.println("Getting Apache Ignite endpoints from: " + url);

            HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();

            conn.setHostnameVerifier(trustAllHosts);

            conn.setSSLSocketFactory(ctx.getSocketFactory());
            conn.addRequestProperty("Authorization", "Bearer " + serviceAccountToken(accountToken));

            // Sending the request and processing a response.
            ObjectMapper mapper = new ObjectMapper();

            Endpoints endpoints = mapper.readValue(conn.getInputStream(), Endpoints.class);

            if (endpoints != null) {
                if (endpoints.subsets != null && !endpoints.subsets.isEmpty()) {
                    for (Subset subset : endpoints.subsets) {

                        if (subset.addresses != null && !subset.addresses.isEmpty()) {
                            for (Address address : subset.addresses) {
                                addrs.add(new InetSocketAddress(address.ip, 0));

                                System.out.println("Added an address to the list: " + address.ip);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to retrieve Ignite pods IP addresses.", e);
        }

        return addrs;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }

    /**
     * Sets the name of Ignite's Kubernetes Service where the IP finder will connect to in order to retrieve IP
     * addresses of existing Ignite pods.
     *
     * @param service Ignite's Kubernetes Service name.
     */
    public void serviceName(String service) {
        this.serviceName = service;
    }

    /**
     * Sets the namespace name Ignite's Kubernetes Service belongs to.
     * If it is not set then 'default' is used as the namespace.
     *
     * @param namespace Ignite's Kubernetes Service namespace name.
     */
    public void namespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Sets Kubernetes master's URL. By default 'kubernetes.default.svc' is used.
     *
     * @param master URL string of Kubernetes master.
     */
    public void masterUrl(String master) {
        this.master = master;
    }

    /**
     * @param accountToken
     */
    public void accountToken(String accountToken) {
        this.accountToken = accountToken;
    }

    /**
     * Kubernetes IP finder initalization.
     *
     * @throws IgniteSpiException In case of error.
     */
    private void init() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true)) {

            if (serviceName == null || serviceName.isEmpty() ||
                namespace == null || namespace.isEmpty() ||
                master == null || master.isEmpty() ||
                accountToken == null || accountToken.isEmpty()) {
                throw new IgniteSpiException(
                    "One or more configuration parameters are invalid [serviceName=" +
                        serviceName + ", namespace=" + namespace + ", masterUrl=" +
                        master + ", accountToken=" + accountToken + "]");
            }

            try {
                // Preparing the URL and SSL context to be used for connection purposes.
                String path = String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, serviceName);

                url = new URL(master + path);

                ctx = SSLContext.getInstance("SSL");

                ctx.init(null, trustAll, new SecureRandom());
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to connect to Ignite's Kubernetes Service.", e);
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            try {
                U.await(initLatch);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            if (url == null || ctx == null)
                throw new IgniteSpiException("IP finder has not been initialized properly.");
        }
    }

    /**
     * @param file
     * @return
     */
    private String serviceAccountToken(String file)  {
        try {
            return new String(Files.readAllBytes(Paths.get(file)));
        } catch (IOException e) {
            throw new IgniteSpiException("Failed to load services account token [accountToken= " + file + "]", e);
        }
    }

    /**
     *
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Address {
        /** */
        public String ip;
    }

    /**
     *
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Subset {
        /** */
        public List<Address> addresses;
    }

    /**
     *
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Endpoints {
        /** */
        public List<Subset> subsets;
    }
}
