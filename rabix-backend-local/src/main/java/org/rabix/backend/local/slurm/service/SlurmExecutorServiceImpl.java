package org.rabix.backend.local.slurm.service;

import com.google.inject.Inject;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.mina.util.ConcurrentHashSet;
import org.rabix.backend.local.slurm.client.SlurmClientException;
import org.rabix.backend.local.slurm.SlurmServiceException;
import org.rabix.backend.local.slurm.client.SlurmClient;
import org.rabix.backend.local.slurm.model.SlurmJob;
import org.rabix.backend.local.slurm.model.SlurmState;
import org.rabix.backend.local.tes.service.TESStorageService;
import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.requirement.Requirement;
import org.rabix.common.helper.JSONHelper;
import org.rabix.common.logging.VerboseLogger;
import org.rabix.executor.engine.EngineStub;
import org.rabix.executor.engine.EngineStubLocal;
import org.rabix.executor.service.ExecutorService;
import org.rabix.executor.status.ExecutorStatusCallback;
import org.rabix.executor.status.ExecutorStatusCallbackException;
import org.rabix.transport.backend.Backend;
import org.rabix.transport.backend.impl.BackendLocal;
import org.rabix.transport.mechanism.TransportPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class SlurmExecutorServiceImpl implements ExecutorService {
    private final static Logger logger = LoggerFactory.getLogger(SlurmExecutorServiceImpl.class);


    private final Set<SlurmExecutorServiceImpl.PendingResult> pendingResults = new ConcurrentHashSet<>();

    private final ScheduledExecutorService scheduledTaskChecker = Executors.newScheduledThreadPool(1);
    private final java.util.concurrent.ExecutorService taskPoolExecutor = Executors.newFixedThreadPool(10);

    private EngineStub<?, ?, ?> engineStub;

    private final Configuration configuration;
    private final ExecutorStatusCallback statusCallback;
    private TESStorageService storageService;
    private SlurmClient slurmClient;

    private class PendingResult {
        private Job job;
        private Future<SlurmJob> future;

        public PendingResult(Job job, Future<SlurmJob> future) {
            this.job = job;
            this.future = future;
        }
    }

    @Inject
    public SlurmExecutorServiceImpl(final SlurmClient slurmClient, final TESStorageService storageService, final ExecutorStatusCallback statusCallback, final Configuration configuration) {
        this.slurmClient = slurmClient;
        this.storageService = storageService;
        this.configuration = configuration;
        this.statusCallback = statusCallback;

        this.scheduledTaskChecker.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (Iterator<SlurmExecutorServiceImpl.PendingResult> iterator = pendingResults.iterator(); iterator.hasNext(); ) {
                    SlurmExecutorServiceImpl.PendingResult pending = (SlurmExecutorServiceImpl.PendingResult) iterator.next();
                    if (pending.future.isDone()) {
                        try {
                            SlurmJob slurmJob = pending.future.get();
                            if (slurmJob.getState().equals(SlurmState.Completed)) {
                                success(pending.job, slurmJob);
                            } else {
                                fail(pending.job, slurmJob);
                            }
                            iterator.remove();
                        } catch (InterruptedException | ExecutionException e) {
                            logger.error("Failed to retrieve SlurmJob", e);
                            handleException(e);
                            iterator.remove();
                        }
                    }
                }
            }

            /**
             * Basic exception handling
             */
            private void handleException(Exception e) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    if (cause.getClass().equals(SlurmServiceException.class)) {
                        Throwable subcause = cause.getCause();
                        if (subcause != null) {
                            if (subcause.getClass().equals(SlurmClientException.class)) {
                                VerboseLogger.log("Failed to communicate with SLURM");
                                System.exit(-10);
                            }
                        }
                    }
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void success(Job job, SlurmJob slurmJob) {
        job = Job.cloneWithStatus(job, Job.JobStatus.COMPLETED);
        Map<String, Object> result = null;
        try {
            result = (Map<String, Object>) FileValue.deserialize(
                    JSONHelper.readMap(
                            slurmJob.getLogs().get(slurmJob.getLogs().size() - 1).getStdout()
                    )
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to process output files: {}", e);
        }

        try {
            result = storageService.transformOutputFiles(result, job.getRootId().toString(), job.getName());
        } catch (BindingException e) {
            logger.error("Failed to process output files", e);
            throw new RuntimeException("Failed to process output files", e);
        }

        job = Job.cloneWithOutputs(job, result);
        job = Job.cloneWithMessage(job, "Success");
        try {
            job = statusCallback.onJobCompleted(job);
        } catch (ExecutorStatusCallbackException e1) {
            logger.warn("Failed to execute statusCallback: {}", e1);
        }
        engineStub.send(job);
    }

    private void fail(Job job, SlurmJob slurmJob) {
        job = Job.cloneWithStatus(job, Job.JobStatus.FAILED);
        try {
            job = statusCallback.onJobFailed(job);
        } catch (ExecutorStatusCallbackException e) {
            logger.warn("Failed to execute statusCallback: {}", e);
        }
        engineStub.send(job);
    }

    @Override
    public void initialize(Backend backend) {
        try {
            switch (backend.getType()) {
                case LOCAL:
                    engineStub = new EngineStubLocal((BackendLocal) backend, this, configuration);
                    break;
                default:
                    throw new TransportPluginException("Backend " + backend.getType() + " is not supported.");
            }
            engineStub.start();
        } catch (TransportPluginException e) {
            logger.error("Failed to initialize Executor", e);
            throw new RuntimeException("Failed to initialize Executor", e);
        }
    }

    public void start(Job job, UUID contextId) {
        pendingResults.add(new SlurmExecutorServiceImpl.PendingResult(job, taskPoolExecutor.submit(new SlurmExecutorServiceImpl.TaskRunCallable(job))));
    }

    @SuppressWarnings("unchecked")
    private <T extends Requirement> T getRequirement(List<Requirement> requirements, Class<T> clazz) {
        for (Requirement requirement : requirements) {
            if (requirement.getClass().equals(clazz)) {
                return (T) requirement;
            }
        }
        return null;
    }


    public class TaskRunCallable implements Callable<SlurmJob> {

        private Job job;

        public TaskRunCallable(Job job) {
            this.job = job;
        }

        @Override
        public SlurmJob call() throws Exception {
            try {
                List<String> initCommand = new ArrayList<>();

                Bindings bindings = BindingsFactory.create(job);
                job = bindings.preprocess(job, storageService.stagingPath(job.getRootId().toString(), job.getName()).toFile(), null);


                storageService.stagingPath(job.getRootId().toString(), job.getName(), "working_dir", "TODO");
                storageService.stagingPath(job.getRootId().toString(), job.getName(), "inputs", "TODO");

                job = storageService.transformInputFiles(job);


                // Write job.json file
                FileUtils.writeStringToFile(
                        storageService.stagingPath(job.getRootId().toString(), job.getName(), "inputs", "job.json").toFile(),
                        JSONHelper.writeObject(job)
                );


                String slurmJobId = slurmClient.runJob(job);

                SlurmJob slurmJob;
                do {
                    Thread.sleep(1000L);
                    slurmJob = slurmClient.getJob(slurmJobId);
                    if (slurmJob == null) {
                        throw new SlurmServiceException("SlurmJob is not created. JobId = " + job.getId());
                    }
                } while (!slurmJob.isFinished());
                return slurmJob;
            } catch (IOException e) {
                logger.error("Failed to write files to SharedFileStorage", e);
                throw new SlurmServiceException("Failed to write files to SharedFileStorage", e);
            } catch (SlurmServiceException e) {
                logger.error("Failed to submit Job to SLURM", e);
                throw new SlurmServiceException("Failed to submit Job to Slurm", e);
            } catch (BindingException e) {
                logger.error("Failed to use Bindings", e);
                throw new SlurmServiceException("Failed to use Bindings", e);
            }
        }

    }

    @Override
    public void stop(List<UUID> ids, UUID contextId) {
        throw new NotImplementedException("This method is not implemented");
    }

    @Override
    public void free(UUID rootId, Map<String, Object> config) {
        throw new NotImplementedException("This method is not implemented");
    }

    @Override
    public void shutdown(Boolean stopEverything) {
        throw new NotImplementedException("This method is not implemented");
    }

    @Override
    public boolean isRunning(UUID id, UUID contextId) {
        throw new NotImplementedException("This method is not implemented");
    }

    @Override
    public Map<String, Object> getResult(UUID id, UUID contextId) {
        throw new NotImplementedException("This method is not implemented");
    }

    @Override
    public boolean isStopped() {
        throw new NotImplementedException("This method is not implemented");
    }

    @Override
    public Job.JobStatus findStatus(UUID id, UUID contextId) {
        throw new NotImplementedException("This method is not implemented");
    }

}
