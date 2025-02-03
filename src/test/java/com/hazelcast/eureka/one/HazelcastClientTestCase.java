/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.eureka.one;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.test.HazelcastTestSupport;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.junit.resource.SimpleEurekaHttpServerResource;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import com.netflix.discovery.shared.transport.SimpleEurekaHttpServer;
import com.netflix.discovery.shared.transport.jersey3.Jersey3TransportClientFactories;
import jakarta.ws.rs.core.MediaType;
import org.apache.commons.configuration.AbstractConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.springframework.cloud.netflix.eureka.http.WebClientTransportClientFactories;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static com.netflix.discovery.shared.transport.EurekaHttpResponse.anEurekaHttpResponse;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * TODO: Fix this tests. They pretend to test Hazelcast Client with Eureka discovery, but
 * that's not what they really do. One method tests Hazelcast Client with multicast and
 * the others test Hazelcast member discovery (not Hazelcast Client!).
 */
public class HazelcastClientTestCase extends HazelcastTestSupport {

    private static final String APP_NAME = "hazelcast-test";

    @Rule
    public SimpleEurekaHttpServerResource resource = new SimpleEurekaHttpServerResource();

    private TestHazelcastFactory factory;
    private SimpleEurekaHttpServer server;
    private EurekaHttpClient requestHandler;

    @Before
    public void setup(){
        server = resource.getEurekaHttpServer();
        requestHandler = resource.getRequestHandler();
        factory = new TestHazelcastFactory();

        reset(requestHandler);

        configure(EurekaOneDiscoveryStrategy.DEFAULT_NAMESPACE, APP_NAME);
        
        EurekaOneDiscoveryStrategyFactory.setEurekaClient(null);
    }

    private void configure(String namespace, String appName){
        URI serviceUri = server.getServiceURI();
        AbstractConfiguration configInstance = ConfigurationManager.getConfigInstance();
        configInstance.setProperty(namespace + ".serviceUrl.default", serviceUri.toString());
        configInstance.setProperty(namespace + ".port", serviceUri.getPort());
        configInstance.setProperty(namespace + ".name", appName);
    }

    @After
    public void tearDown(){
        if(null != server){
            server.shutdown();
        }

        if(null != factory){
            factory.shutdownAll();
        }
    }

    @Test
    public void testInstanceRegistration(){

        EurekaHttpResponse<Applications> response = generateMockResponse(Collections.emptyList());
        when(requestHandler.getApplications()).thenReturn(response);

        factory.newHazelcastInstance();

        ArgumentCaptor<InstanceInfo> captor = ArgumentCaptor.forClass(InstanceInfo.class);
        verify(requestHandler, timeout(5000).times(1)).register(captor.capture());

        assertNotNull(captor.getAllValues());
        assertEquals(captor.getAllValues().size(), 1);

        InstanceInfo actual = captor.getValue();
        assertEquals(actual.getAppName().toLowerCase(), APP_NAME);

    }

    @Test
    public void testSimpleDiscovery(){
        ArgumentCaptor<InstanceInfo> captor = ArgumentCaptor.forClass(InstanceInfo.class);

        EurekaHttpResponse<Applications> response = generateMockResponse(Collections.emptyList());
        when(requestHandler.getApplications()).thenReturn(response);

        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        verify(requestHandler, timeout(5000).times(2)).register(captor.capture());
        response = generateMockResponse(captor.getAllValues());
        when(requestHandler.getApplications()).thenReturn(response);

        assertClusterSizeEventually(2, hz1);
        assertClusterSizeEventually(2, hz2);

        HazelcastInstance client = factory.newHazelcastClient();
        reset(requestHandler);
        when(requestHandler.getApplications()).thenReturn(response);
        verify(requestHandler, timeout(1000).times(0)).register(ArgumentMatchers.any());
        assertClusterSizeEventually(2, client);
    }

    @Test
    public void testInstanceDown(){
        ArgumentCaptor<InstanceInfo> captor = ArgumentCaptor.forClass(InstanceInfo.class);

        EurekaHttpResponse<Applications> response = generateMockResponse(Collections.emptyList());
        when(requestHandler.getApplications()).thenReturn(response);

        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        verify(requestHandler, timeout(5000).times(2)).register(captor.capture());
        response = generateMockResponse(captor.getAllValues());
        when(requestHandler.getApplications()).thenReturn(response);

        hz1.shutdown();

        ArgumentCaptor<String> id = ArgumentCaptor.forClass(String.class);
        verify(requestHandler, timeout(5000).times(1)).cancel(anyString(), id.capture());

        reset(requestHandler);
        for(InstanceInfo info : captor.getAllValues()){
            if(info.getId().equals(id.getValue())){
                captor.getAllValues().remove(info);
                break;
            }
        }

        response = generateMockResponse(captor.getAllValues());
        when(requestHandler.getApplications()).thenReturn(response);

        assertClusterSizeEventually(1, hz2);
    }


    @Test
    public void testExternalRegistration(){
        EurekaHttpResponse<Applications> response = generateMockResponse(Collections.emptyList());
        when(requestHandler.getApplications()).thenReturn(response);

        Config config = new XmlConfigBuilder().build();
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        DiscoveryStrategyConfig strategyConfig = discoveryConfig.getDiscoveryStrategyConfigs().iterator().next();
        strategyConfig.addProperty("self-registration", "false");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(2, hz1);
        assertClusterSizeEventually(2, hz2);

        verify(requestHandler, after(5000).never()).register(any(InstanceInfo.class));

    }

    @Test
    public void testNamespaceRegistration(){
        final String appName = "other";
        final String namespace = "hz";
        configure(namespace, appName);

        EurekaHttpResponse<Applications> response = generateMockResponse(Collections.emptyList(), appName);
        when(requestHandler.getApplications()).thenReturn(response);

        Config config = new XmlConfigBuilder().build();
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        DiscoveryStrategyConfig strategyConfig = discoveryConfig.getDiscoveryStrategyConfigs().iterator().next();
        strategyConfig.addProperty("namespace", namespace);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(1, hz1);

        ArgumentCaptor<InstanceInfo> captor = ArgumentCaptor.forClass(InstanceInfo.class);
        verify(requestHandler, timeout(5000).atLeastOnce()).register(captor.capture());

        String actual = captor.getValue().getAppName().toLowerCase();
        assertThat(actual, is(appName));

    }
    
    @Test
    public void testInstanceRegistrationUsingProvidedEurekaClient() {
        EurekaClient eurekaClient = mock(EurekaClient.class);
        ApplicationInfoManager applicationInfoManager = mock(ApplicationInfoManager.class);
        EurekaInstanceConfig eurekaInstanceConfig = mock(EurekaInstanceConfig.class);

        when(eurekaClient.getApplicationInfoManager()).thenReturn(applicationInfoManager);
        when(eurekaClient.getApplication(anyString())).thenReturn(new Application(APP_NAME));

        InstanceInfo instanceInfo = InstanceInfo.Builder.newBuilder().setAppName(APP_NAME).build();
        when(applicationInfoManager.getInfo()).thenReturn(instanceInfo);
        when(applicationInfoManager.getEurekaInstanceConfig()).thenReturn(eurekaInstanceConfig);
        when(eurekaInstanceConfig.getAppname()).thenReturn(APP_NAME);

        // use provided EurekaClient
        EurekaOneDiscoveryStrategyFactory.setEurekaClient(eurekaClient);
        EurekaOneDiscoveryStrategyFactory.setGroupName("dev");

        Config config = new XmlConfigBuilder().build();
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        DiscoveryStrategyConfig strategyConfig = discoveryConfig.getDiscoveryStrategyConfigs().iterator().next();
        strategyConfig.addProperty("use-metadata-for-host-and-port", false);
        
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        
        verify(eurekaClient, times(2)).getApplicationInfoManager();
        verify(eurekaClient, times(2)).getApplication(APP_NAME);
        verify(applicationInfoManager, atLeastOnce()).setInstanceStatus(InstanceStatus.UP);

        assertClusterSizeEventually(2, hz1);
        assertClusterSizeEventually(2, hz2);
    }

    
    @Test
    public void testInstanceRegistrationUsingProvidedEurekaClientAndMetadata() {
        EurekaClient eurekaClient = mock(EurekaClient.class);
        ApplicationInfoManager applicationInfoManager = mock(ApplicationInfoManager.class);
        EurekaInstanceConfig eurekaInstanceConfig = mock(EurekaInstanceConfig.class);

        when(eurekaClient.getApplicationInfoManager()).thenReturn(applicationInfoManager);
        when(eurekaClient.getApplication(anyString())).thenReturn(new Application(APP_NAME));

        InstanceInfo instanceInfo = InstanceInfo.Builder.newBuilder().setAppName(APP_NAME).build();
		when(applicationInfoManager.getInfo()).thenReturn(instanceInfo);
        when(applicationInfoManager.getEurekaInstanceConfig()).thenReturn(eurekaInstanceConfig);
        when(eurekaInstanceConfig.getAppname()).thenReturn(APP_NAME);

        // use provided EurekaClient
        EurekaOneDiscoveryStrategyFactory.setEurekaClient(eurekaClient);
        EurekaOneDiscoveryStrategyFactory.setGroupName("dev");

        Config config = new XmlConfigBuilder().build();
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        DiscoveryStrategyConfig strategyConfig = discoveryConfig.getDiscoveryStrategyConfigs().iterator().next();
        strategyConfig.addProperty("use-metadata-for-host-and-port", true);
        
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        
        verify(eurekaClient, times(2)).getApplicationInfoManager();
        verify(eurekaClient, times(2)).getApplication(APP_NAME);
        verify(applicationInfoManager, atLeastOnce()).setInstanceStatus(InstanceStatus.UP);

        assertClusterSizeEventually(2, hz1);
        assertClusterSizeEventually(2, hz2);
    }

    @Test
    public void testInstanceRegistrationWithJersey3TransportClient() {
        EurekaClient eurekaClient = mock(EurekaClient.class);
        ApplicationInfoManager applicationInfoManager = mock(ApplicationInfoManager.class);
        EurekaInstanceConfig eurekaInstanceConfig = mock(EurekaInstanceConfig.class);
        ILogger mockLogger = mock(ILogger.class);
        DiscoveryNode mockDiscoveryNode = mock(DiscoveryNode.class);

        // Use the real Jersey3 transport client factories
        Jersey3TransportClientFactories jersey3TransportFactories = new Jersey3TransportClientFactories();

        when(eurekaClient.getApplicationInfoManager()).thenReturn(applicationInfoManager);
        when(eurekaClient.getApplication(anyString())).thenReturn(new Application(APP_NAME));

        InstanceInfo instanceInfo = InstanceInfo.Builder.newBuilder().setAppName(APP_NAME).build();
        when(applicationInfoManager.getInfo()).thenReturn(instanceInfo);
        when(applicationInfoManager.getEurekaInstanceConfig()).thenReturn(eurekaInstanceConfig);
        when(eurekaInstanceConfig.getAppname()).thenReturn(APP_NAME);

        // Set the Eureka client and Jersey3 transport factories
        EurekaOneDiscoveryStrategyFactory.setEurekaClient(eurekaClient);
        EurekaOneDiscoveryStrategyFactory.setGroupName("dev");
        EurekaOneDiscoveryStrategyFactory.setTransportClientFactories(jersey3TransportFactories);

        EurekaOneDiscoveryStrategyFactory factory = new EurekaOneDiscoveryStrategyFactory();
        DiscoveryStrategy strategy = factory.newDiscoveryStrategy(mockDiscoveryNode, mockLogger, Collections.emptyMap());
        assertNotNull("Discovery strategy should be created", strategy);

        Config config = new XmlConfigBuilder().build();
        HazelcastInstance hz1 = this.factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = this.factory.newHazelcastInstance(config);

        assertClusterSizeEventually(2, hz1);
        assertClusterSizeEventually(2, hz2);

        verify(eurekaClient, times(3)).getApplicationInfoManager();
        verify(eurekaClient, times(2)).getApplication(APP_NAME);
        verify(applicationInfoManager, atLeastOnce()).setInstanceStatus(InstanceStatus.UP);
    }

    @Test
    public void testInstanceRegistrationWithWebClientTransportClient() {
        EurekaClient eurekaClient = mock(EurekaClient.class);
        ApplicationInfoManager applicationInfoManager = mock(ApplicationInfoManager.class);
        EurekaInstanceConfig eurekaInstanceConfig = mock(EurekaInstanceConfig.class);
        ILogger mockLogger = mock(ILogger.class);
        DiscoveryNode mockDiscoveryNode = mock(DiscoveryNode.class);

        // Use the real Web Client transport client factories
        Supplier<WebClient.Builder> webClientBuilderSupplier =
                org.springframework.web.reactive.function.client.WebClient::builder;

        WebClientTransportClientFactories webClientTransportFactories =
                new WebClientTransportClientFactories(webClientBuilderSupplier);

        when(eurekaClient.getApplicationInfoManager()).thenReturn(applicationInfoManager);
        when(eurekaClient.getApplication(anyString())).thenReturn(new Application(APP_NAME));

        InstanceInfo instanceInfo = InstanceInfo.Builder.newBuilder().setAppName(APP_NAME).build();
        when(applicationInfoManager.getInfo()).thenReturn(instanceInfo);
        when(applicationInfoManager.getEurekaInstanceConfig()).thenReturn(eurekaInstanceConfig);
        when(eurekaInstanceConfig.getAppname()).thenReturn(APP_NAME);

        // Set the Eureka client and transport factories
        EurekaOneDiscoveryStrategyFactory.setEurekaClient(eurekaClient);
        EurekaOneDiscoveryStrategyFactory.setGroupName("dev");
        EurekaOneDiscoveryStrategyFactory.setTransportClientFactories(webClientTransportFactories);

        EurekaOneDiscoveryStrategyFactory factory = new EurekaOneDiscoveryStrategyFactory();
        DiscoveryStrategy strategy = factory.newDiscoveryStrategy(mockDiscoveryNode, mockLogger, Collections.emptyMap());
        assertNotNull("Discovery strategy should be created", strategy);

        Config config = new XmlConfigBuilder().build();
        HazelcastInstance hz1 = this.factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = this.factory.newHazelcastInstance(config);

        assertClusterSizeEventually(2, hz1);
        assertClusterSizeEventually(2, hz2);

        verify(eurekaClient, times(3)).getApplicationInfoManager();
        verify(eurekaClient, times(2)).getApplication(APP_NAME);
        verify(applicationInfoManager, atLeastOnce()).setInstanceStatus(InstanceStatus.UP);
    }

    private EurekaHttpResponse<Applications> generateMockResponse(List<InstanceInfo> infoList) {
        return generateMockResponse(infoList, APP_NAME);
    }

    private EurekaHttpResponse<Applications> generateMockResponse(List<InstanceInfo> infoList, String appName){

        Application application = new Application();
        application.setName(appName);
        for(InstanceInfo info : infoList){
            application.addInstance(info);
        }

        Applications applications = new Applications();
        applications.addApplication(application);

        return anEurekaHttpResponse(200, applications)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
