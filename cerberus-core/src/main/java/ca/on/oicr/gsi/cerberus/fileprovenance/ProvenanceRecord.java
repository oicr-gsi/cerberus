package ca.on.oicr.gsi.cerberus.fileprovenance;

import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.api.ProvenanceWorkflowRun;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

public final class ProvenanceRecord<T extends LimsProvenance> {

  public abstract static class Mapper<R, L extends LimsProvenance> {
    private final Class<L> subclass;

    protected Mapper(Class<L> subclass) {
      this.subclass = subclass;
    }

    protected abstract R apply(L lims);

    public Optional<R> tryApply(LimsProvenance input) {
      return subclass.isInstance(input)
          ? Optional.ofNullable(apply(subclass.cast(input)))
          : Optional.empty();
    }
  }

  private final int formatRevision;
  private final T lims;
  private final String provider;
  private final ProvenanceAnalysisRecord<ExternalId> record;
  private final ProvenanceWorkflowRun<ExternalKey> workflow;

  public ProvenanceRecord(
      String provider,
      int formatRevision,
      T lims,
      ProvenanceWorkflowRun<ExternalKey> workflow,
      ProvenanceAnalysisRecord<ExternalId> record) {
    this.formatRevision = formatRevision;
    this.lims = lims;
    this.workflow = workflow;
    this.record = record;
    this.provider = provider;
  }

  @SafeVarargs
  public final <R> Optional<R> apply(Mapper<R, ? extends T>... mappers) {
    return Stream.of(mappers).flatMap(m -> m.tryApply(lims).stream()).findFirst();
  }

  public <S extends T> boolean asSubtype(Class<S> clazz, Consumer<ProvenanceRecord<S>> consumer) {
    if (clazz.isInstance(lims)) {
      consumer.accept(
          new ProvenanceRecord<>(provider, formatRevision, clazz.cast(lims), workflow, record));
      return true;
    } else {
      return false;
    }
  }

  public int formatRevision() {
    return formatRevision;
  }

  public T lims() {
    return lims;
  }

  public String provider() {
    return provider;
  }

  public ProvenanceAnalysisRecord<ExternalId> record() {
    return record;
  }

  public ProvenanceWorkflowRun<ExternalKey> workflow() {
    return workflow;
  }
}
