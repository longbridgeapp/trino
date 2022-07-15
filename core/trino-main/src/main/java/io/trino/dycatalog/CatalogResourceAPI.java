/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.dycatalog;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import io.trino.connector.ConnectorManager;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.DiscoveryNodeManager;
import io.trino.server.security.ResourceSecurity;
import org.testng.collections.Lists;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;

/**
 * @author rock
 */
@Path("/trino/catalog")
public class CatalogResourceAPI
{
    private final ConnectorManager connectorManager;
    private final Announcer announcer;
    private final CatalogManager catalogManager;
    private final DiscoveryNodeManager discoveryNodeManager;

    Logger log = Logger.get(CatalogResourceAPI.class);

    @Inject
    public CatalogResourceAPI(ConnectorManager connectorManager,
                              Announcer announcer, CatalogManager catalogManager
        , DiscoveryNodeManager discoveryNodeManager)
    {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.announcer = requireNonNull(announcer, "announcer is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.discoveryNodeManager = requireNonNull(discoveryNodeManager, "discoveryNodeManager is null");
    }

    //method for add catalog
    @ResourceSecurity(PUBLIC)
    @POST
    @Path("/addCatalog")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createCatalog(CatalogEntity catalogEntity)
    {
        requireNonNull(catalogEntity, "catalogInfo is null");
        log.info("### RestApi Add Catalog %s using connector %s --", catalogEntity.getCatalogName(), catalogEntity.getConnectorName());
        refreshConnector(CatalogOperationEnum.CATALOG_ADD.getKey(), Lists.newArrayList(catalogEntity));
        return Response.status(Response.Status.OK).entity("success").build();
    }

    @ResourceSecurity(PUBLIC)
    @POST
    @Path("/addCatalogs")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createCatalogs(CatalogEntitySet catalogEntitySet)
    {
        requireNonNull(catalogEntitySet, "catalogEntitySet is null");

        log.info("### RestApi Add Catalogs");
        refreshConnector(CatalogOperationEnum.CATALOG_MUL_ADD.getKey(), catalogEntitySet.getCatalogEntitys());
        return Response.status(Response.Status.OK).entity("success").build();
    }

    @ResourceSecurity(PUBLIC)
    @POST
    @Path("/removeCatalog")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeCatalog(CatalogEntity catalogEntity){
        String catalogName = catalogEntity.getCatalogName();
        log.info("### RestApi Remove Catalog %s ", catalogName);
        refreshConnector(CatalogOperationEnum.CATALOG_DELETE.getKey(), Lists.newArrayList(catalogEntity));
        return Response.status(Response.Status.OK).entity("success").build();
    }

    @ResourceSecurity(PUBLIC)
    @POST
    @Path("/updateCatalog")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateCatalog(CatalogEntity catalogEntity){
        String catalogName = catalogEntity.getCatalogName();
        if(catalogEntity.getOrigCatalogName()!=null
            && !"".equals(catalogEntity.getOrigCatalogName())){
            log.info("### RestApi update Catalog %s, Original Catalog %s", catalogName, catalogEntity.getOrigCatalogName());
        }else{
            log.info("### RestApi update Catalog %s ", catalogName);
        }
        refreshConnector(CatalogOperationEnum.CATALOG_UPDATE.getKey(), Lists.newArrayList(catalogEntity));
        return Response.status(Response.Status.OK).entity("success").build();
    }

    @ResourceSecurity(PUBLIC)
    @POST
    @Path("/searchCatalog")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response searchCatalog(CatalogEntity catalogEntity)
    {
        requireNonNull(catalogEntity, "catalogInfo is null");
        log.info("### RestApi search Catalog %s --", catalogEntity.getCatalogName());
        Optional<Catalog> catalog = catalogManager.getCatalog(catalogEntity.getCatalogName());
        return Response.status(Response.Status.OK).entity(catalog.isEmpty() ? null : catalog.get().toString()).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("/ping")
    @Produces(MediaType.APPLICATION_JSON)
    public Response ping(){
        return Response.status(Response.Status.OK).entity("pong").build();
    }

    private void refreshConnector(String key, List<CatalogEntity> catalogEntitys){
        // get existing announcement
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());
        Map<String, String> mapProperties = new HashMap<String,String>(announcement.getProperties());//？？？
        // 在ConnectorManager 中更新数据源实例的信息
        connectorManager.refreshConnector(key, catalogEntitys);
        // 基于更新的数据源实例信息，构建新的ServiceAnnouncement
        // automatically build connectorIds if not configured
        Set<String> connectorIds = catalogManager.getCatalogs().stream()
            .map(Catalog::getConnectorCatalogName)
            .map(Object::toString)
            .collect(toImmutableSet());
//        Set<String> connectorIds = catalogManager.getCatalogNames(); // 构建catalog名称的集合

        // build announcement with updated sources
        ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
//        Iterator<ServiceAnnouncement> iter =announcer.getServiceAnnouncements().iterator();
//        while (iter.hasNext()){
//            ServiceAnnouncement sa = iter.next();
//            if(sa.getId()==announcement.getId()){
//                builder.addProperties(sa.getProperties());
//            }
//        }
        mapProperties.replace("connectorIds",Joiner.on(',').join(connectorIds));
        builder.addProperties(mapProperties);
//        builder.addProperty("connectorIds",Joiner.on(',').join(connectorIds));
        // update announcement
        // 广播数据源信息
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
        // 强制发出广播
        announcer.forceAnnounce();
        // 在心跳信息中更新数据源信息
        discoveryNodeManager.updateCatalogToActiveConnectorNodes(key, catalogEntitys);

    }

    private static ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("trino")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Trino announcement not found: " + announcements);
    }
}
