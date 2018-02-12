// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SolrDocumentBuilder.cs" company="Sitecore">
//   Copyright (c) Sitecore. All rights reserved.
// </copyright>
// <summary>
//   Defines the SolrDocumentBuilder type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using Sitecore.ContentSearch.Abstractions;

namespace Sitecore.Support.ContentSearch.SolrProvider
{
  using System;
  using System.Collections.Concurrent;
  using System.Globalization;
  using System.Threading.Tasks;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Diagnostics;
  using Sitecore.ContentSearch.SolrProvider;

  /// <summary>
  /// The solr document builder.
  /// </summary>
  public class SolrDocumentBuilder : Sitecore.ContentSearch.SolrProvider.SolrDocumentBuilder
  {
    private readonly SolrFieldNameTranslator fieldNameTranslator;

    private readonly CultureInfo culture;

    private readonly ISettings settings;

    private readonly IProviderUpdateContext Context;

    /// <summary>
    /// Initializes a new instance of the <see cref="SolrDocumentBuilder" /> class.
    /// </summary>
    /// <param name="indexable">The indexable.</param>
    /// <param name="context">The context.</param>
    public SolrDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {
    }

    public override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");
        base.Indexable.LoadAllFields();
        if (base.IsParallel)
        {
          ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
          Parallel.ForEach<IIndexableDataField>(base.Indexable.Fields, base.ParallelOptions,
            delegate(IIndexableDataField f)
            {
              try
              {
                this.CheckAndAddField(this.Indexable, f);
              }
              catch (Exception exception)
              {
                exceptions.Enqueue(exception);
              }
            });
          if (exceptions.Count > 0)
          {
            throw new AggregateException(exceptions);
          }
        }
        else
        {
          foreach (IIndexableDataField field in base.Indexable.Fields)
          {
            this.CheckAndAddField(base.Indexable, field);
          }
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }

    private void CheckAndAddField(IIndexable indexable, IIndexableDataField field)
    {
      string name = field.Name;
      if ((base.IsTemplate && base.Options.HasExcludedTemplateFields) &&
          (base.Options.ExcludedTemplateFields.Contains(name) ||
           base.Options.ExcludedTemplateFields.Contains(field.Id.ToString())))
      {
        VerboseLogging.CrawlingLogDebug(() =>
          $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was excluded.");
      }
      else if ((base.IsMedia && base.Options.HasExcludedMediaFields) &&
               base.Options.ExcludedMediaFields.Contains(field.Name))
      {
        VerboseLogging.CrawlingLogDebug(() =>
          $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Media field was excluded.");
      }
      else if (base.Options.ExcludedFields.Contains(field.Id.ToString()) || base.Options.ExcludedFields.Contains(name))
      {
        VerboseLogging.CrawlingLogDebug(() =>
          $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was excluded.");
      }
      else
      {
        try
        {
          if (base.Options.IndexAllFields)
          {
            this.AddField(field);
          }
          else if (base.Options.IncludedFields.Contains(name) ||
                   base.Options.IncludedFields.Contains(field.Id.ToString()))
          {
            this.AddField(field);
          }
          else
          {
            VerboseLogging.CrawlingLogDebug(() =>
              $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was not included.");
          }
        }
        catch (Exception exception)
        {
          if (base.Settings.StopOnCrawlFieldError())
          {
            throw;
          }
          CrawlingLog.Log.Fatal(
            string.Format("Could not add field {1} : {2} for indexable {0}", indexable.UniqueId, field.Id, field.Name),
            exception);
        }
      }
    }
  }

}
