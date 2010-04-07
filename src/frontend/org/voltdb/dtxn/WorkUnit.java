/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState.WorkUnitState;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState.WorkUnitState.DependencyState;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.VoltMessage;

/**
 * <p>A <code>WorkUnit</code> represents some "work" to be done
 * as part of a transaction. Each <code>WorkUnit</code> has a
 * list of dependencies that must be met before it can be passed
 * to a worker site to be worked on.</p>

 * <p>The interface presented is primarily for the worker site,
 * as there is no method-based way to add dependencies and no place
 * to store important info like transaction ids. The chosen txn
 * system should probably subclass <code>WorkUnit</code> to taste.
 *
 */
class WorkUnit
{
    class DependencyTracker
    {
        HashMap<Integer, VoltTable> m_results;
        int m_depId;
        int m_expectedDeps;
        HashSet<Integer> m_expectedSites;

        DependencyTracker(int depId, int expectedDeps,
                          HashSet<Integer> expectedSites)
        {
            m_depId = depId;
            m_results = new HashMap<Integer, VoltTable>();
            m_expectedDeps = expectedDeps;
            m_expectedSites = expectedSites;
        }

        boolean addResult(int siteId, int mapId, VoltTable result)
        {
            boolean retval = true;
            if (!(m_results.containsKey(mapId)))
            {
                m_results.put(mapId, result);
            }
            else
            {
                if (!m_results.get(mapId).hasSameContents(result))
                {
                    retval = false;
                }
            }
            m_expectedSites.remove(siteId);
            m_expectedDeps--;
            return retval;
        }

        void removeSite(int siteId)
        {
            // This is a really horrible hack to work around the fact that
            // we don't know the set of remote sites from which to expect
            // results for any non-local transaction type other than a
            // multi-partition transaction that goes to all the sites in the
            // cluster.  This will keep normal transactions running if
            // a failure occurs while they're in progress, but system procedures
            // that depend on odd dependency sets (per-node, 1-1, etc) will
            // not handle site failures (yet).
            if ((m_depId & DtxnConstants.MULTIPARTITION_DEPENDENCY) != 0)
            {
                if (m_expectedSites.contains((Integer) siteId))
                {
                    m_expectedSites.remove((Integer) siteId);
                }
            }
        }

        int size()
        {
            return m_results.size();
        }

        boolean isSatisfied()
        {
            if ((m_depId & DtxnConstants.MULTIPARTITION_DEPENDENCY) != 0)
            {
                return (m_expectedSites.size() == 0);
            }
            else
            {
                return (m_expectedDeps == 0);
            }
        }

        VoltTable getResult(int mapId)
        {
            return m_results.get(mapId);
        }

        List<VoltTable> getResults()
        {
            ArrayList<VoltTable> retval =
                new ArrayList<VoltTable>(m_results.values());
            return retval;
        }

        public int getExpectedDepCount()
        {
            return m_expectedDeps;
        }
    }

    /**
     * The list of dependencies for this <code>WorkUnit</code>.
     * The map is hashed by dependency ID
     */
    HashMap<Integer, DependencyTracker> m_dependencies = null;

    /**
     * A VoltMessage subclass representing work to be done.
     * A null payload means commit the txn.
     */
    VoltMessage m_payload = null;

    /**
     * What kind of FragmentTask type, if any, does this WorkUnit represent.
     * Used to determine what sort of duplicate suppression to perform for sysprocs
     */
    int m_taskType = FragmentTaskMessage.USER_PROC;

    /**
     * Does this workunit indicate that a running stored procedure
     * that was paused should be resumed?
     */
    boolean m_shouldResumeProcedure = false;

    int m_stackCount = 0;
    boolean commitEvenIfDirty = false;
    boolean nonTransactional = false;

    /**
     * Get the "payload" for this <code>WorkUnit</code>. The payload is
     * an subclass of VoltMessage that contains instructions on what to
     * do.
     *
     * @return The non-null VoltMessage subclass.
     */
    VoltMessage getPayload() {
        return m_payload;
    }

    /**
     * Get IDs of all dependency objects.
     * @return IDs of dependency objects in no particular order. Returns null
     * if there are no dependencies
     */
    Set<Integer> getDependencyIds() {
        if (m_dependencies == null) {
            return null;
        }
        return m_dependencies.keySet();
    }

    /**
     * Get a list of VoltTable dependencies for a dependency id.
     *
     * @param dependencyId The id of the requested dependency list.
     * @return A list of dependencies as VoltTables or null if there
     * is no match for the id given.
     */
    List<VoltTable> getDependency(int dependencyId) {
        if (m_dependencies == null) {
            return null;
        }
        List<VoltTable> retval = new ArrayList<VoltTable>();
        DependencyTracker dep_tracker = m_dependencies.get(dependencyId);
        if (dep_tracker != null)
        {
            retval = dep_tracker.getResults();
        }
        return retval;
    }

    /**
     * Get a map of VoltTable dependencies hashed by dependency id.
     */
    HashMap<Integer, List<VoltTable>> getDependencies()
    {
        if (m_dependencies == null)
        {
            return null;
        }
        HashMap<Integer, List<VoltTable>> retval = new HashMap<Integer, List<VoltTable>>();
        for (int dep_id : m_dependencies.keySet())
        {
           retval.put(dep_id, getDependency(dep_id));
        }
        return retval;
    }

    /**
     * Does this workunit indicate that a running stored procedure
     * that was paused should be resumed?
     *
     * @return Whether or not to resume a stalled proceudre.
     */
    boolean shouldResumeProcedure() {
        return m_shouldResumeProcedure;
    }

    WorkUnit(SiteTracker siteTracker, VoltMessage payload,
             int[] dependencyIds, int siteId,
             int[] nonCoordinatingSiteIds,
             boolean shouldResumeProcedure)
    {
        this.m_payload = payload;
        m_shouldResumeProcedure = shouldResumeProcedure;
        if (payload != null && payload instanceof FragmentTaskMessage)
        {
            m_taskType = ((FragmentTaskMessage) payload).getFragmentTaskType();
        }

        if (dependencyIds != null && dependencyIds.length > 0) {
            m_dependencies = new HashMap<Integer, DependencyTracker>();
            for (int dependency : dependencyIds) {
                int depsToExpect = 1;
                HashSet<Integer> expected_sites = new HashSet<Integer>();
                expected_sites.add(siteId);
                if ((dependency & DtxnConstants.MULTIPARTITION_DEPENDENCY) != 0) {
                    depsToExpect = siteTracker.getLiveSiteCount();
                    for (Integer site_id : nonCoordinatingSiteIds)
                    {
                        expected_sites.add(site_id);
                    }
                }
                else if ((dependency & DtxnConstants.MULTINODE_DEPENDENCY) != 0) {
                    depsToExpect = siteTracker.getLiveInitiatorCount();
                }
                m_dependencies.put(dependency,
                                   new DependencyTracker(dependency,
                                                         depsToExpect,
                                                         expected_sites));
            }
        }
    }

    void putDependency(int dependencyId, int siteId, VoltTable payload) {
        assert payload != null;
        assert m_dependencies != null;
        assert m_dependencies.containsKey(dependencyId);
        assert m_dependencies.get(dependencyId) != null;

        int partition = VoltDB.instance().getCatalogContext().siteTracker.getPartitionForSite(siteId);
        int map_id = partition;
        if (m_taskType == FragmentTaskMessage.SYS_PROC_PER_SITE)
        {
            map_id = siteId;
        }

        boolean duplicate_okay =
            m_dependencies.get(dependencyId).addResult(siteId, map_id, payload);
        if (!duplicate_okay)
        {
            String msg = "Mismatched results received for partition: " + partition;
            msg += "\n  from execution site: " + siteId;
            msg += "\n  Original results: " + m_dependencies.get(dependencyId).getResult(map_id).toString();
            msg += "\n  Mismatched results: " + payload.toString();
            throw new RuntimeException(msg);
        }
    }

    boolean allDependenciesSatisfied() {
        boolean satisfied = true;
        if (m_dependencies != null)
        {
            for (DependencyTracker tracker : m_dependencies.values())
            {
                if (!tracker.isSatisfied())
                {
                    satisfied = false;
                }
            }
        }
        return satisfied && (m_stackCount == 0);
    }

    void removeSite(int siteId)
    {
        if (m_dependencies != null)
        {
            for (DependencyTracker tracker : m_dependencies.values())
            {
                tracker.removeSite(siteId);
            }
        }
    }

    /** Return a simplified, flat version of this objects state for dumping */
    WorkUnitState getDumpContents() {
        WorkUnitState retval = new WorkUnitState();

        if (m_payload != null)
            retval.payload = m_payload.toString();
        retval.shouldResume = m_shouldResumeProcedure;
        retval.outstandingDependencyCount = 0;
        if (m_dependencies != null) {
            retval.dependencies = new DependencyState[m_dependencies.size()];
            int i = 0;
            for (Entry<Integer, DependencyTracker> entry : m_dependencies.entrySet())
            {
                DependencyState ds = new DependencyState();
                ds.dependencyId = entry.getKey();
                ds.count = entry.getValue().size();
                retval.outstandingDependencyCount += entry.getValue().getExpectedDepCount();
                retval.dependencies[i++] = ds;
            }
        }

        return retval;
    }
}
