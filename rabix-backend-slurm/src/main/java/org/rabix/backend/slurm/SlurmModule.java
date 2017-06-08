package org.rabix.backend.slurm;

import com.google.inject.Scopes;
import org.rabix.backend.api.BackendModule;
import org.rabix.backend.api.WorkerService;
import org.rabix.backend.slurm.client.SlurmClient;
import org.rabix.backend.slurm.service.SlurmWorkerServiceImpl;
import org.rabix.backend.tes.service.TESStorageService;
import org.rabix.backend.tes.service.impl.LocalTESStorageServiceImpl;
import org.rabix.common.config.ConfigModule;

public class SlurmModule extends BackendModule {

  public SlurmModule(ConfigModule configModule) {
    super(configModule);
  }

  @Override
  protected void configure() {
    bind(SlurmClient.class).in(Scopes.SINGLETON);
    bind(TESStorageService.class).to(LocalTESStorageServiceImpl.class).in(Scopes.SINGLETON);
    bind(WorkerService.class).annotatedWith(SlurmWorkerServiceImpl.SlurmWorker.class).to(SlurmWorkerServiceImpl.class).in(Scopes.SINGLETON);
  }

}
