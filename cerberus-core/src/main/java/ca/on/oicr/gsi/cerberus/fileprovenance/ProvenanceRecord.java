package ca.on.oicr.gsi.cerberus.fileprovenance;

import ca.on.oicr.gsi.cerberus.pinery.ExternalIdVersion;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.api.ProvenanceWorkflowRun;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A joined file provenance record
 *
 * @param <T> the subtype of LIMS information associated with this record
 */
public final class ProvenanceRecord<T extends LimsProvenance> {

  /**
   * Extract a value to a different type based on the subtype of LIMS data provided
   *
   * @param <R> the data type returned
   * @param <L> the LIMS provenance type required
   */
  public abstract static class Mapper<R, L extends LimsProvenance> {
    private final Class<L> subclass;

    protected Mapper(Class<L> subclass) {
      this.subclass = subclass;
    }

    /**
     * Extract the LIMS value required
     *
     * @param lims the appropriate subtype
     * @return the extracted value
     */
    protected abstract R apply(L lims);

    public final Optional<R> tryApply(LimsProvenance input) {
      return subclass.isInstance(input)
          ? Optional.ofNullable(apply(subclass.cast(input)))
          : Optional.empty();
    }
  }

  private final int formatRevision;
  private final T lims;
  private final String provider;
  private final ExternalIdVersion currentExternalIdVersion;
  private final ProvenanceAnalysisRecord<ExternalId> record;
  private final ProvenanceWorkflowRun<ExternalKey> workflow;

  public ProvenanceRecord(
      String provider,
      int formatRevision,
      T lims,
      ExternalIdVersion currentExternalIdVersion,
      ProvenanceWorkflowRun<ExternalKey> workflow,
      ProvenanceAnalysisRecord<ExternalId> record) {
    this.formatRevision = formatRevision;
    this.lims = lims;
    this.workflow = workflow;
    this.record = record;
    this.provider = provider;
    this.currentExternalIdVersion = currentExternalIdVersion;
  }

  /**
   * Extract a different value depending on the type of LIMS data
   *
   * @param mappers the conversions to use
   * @param <R> the result type required
   * @return the resulting value of conversion if one can be found
   */
  @SafeVarargs
  public final <R> Optional<R> apply(Mapper<R, ? extends T>... mappers) {
    return Stream.of(mappers).flatMap(m -> m.tryApply(lims).stream()).findFirst();
  }

  /**
   * Perform a callback depending on the type of LIMS data
   *
   * @param clazz the type of LIMS data required
   * @param consumer a callback to handle the data if this is the required type
   * @param <S> type of of LIMS data required
   * @return true if the LIMS data matched this type
   */
  public <S extends T> boolean asSubtype(Class<S> clazz, Consumer<ProvenanceRecord<S>> consumer) {
    if (clazz.isInstance(lims)) {
      consumer.accept(
          new ProvenanceRecord<>(
              provider,
              formatRevision,
              clazz.cast(lims),
              currentExternalIdVersion,
              workflow,
              record));
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

  public ExternalIdVersion currentExternalIdVersion() {
    return currentExternalIdVersion;
  }

  public ProvenanceAnalysisRecord<ExternalId> record() {
    return record;
  }

  public ProvenanceWorkflowRun<ExternalKey> workflow() {
    return workflow;
  }
}
