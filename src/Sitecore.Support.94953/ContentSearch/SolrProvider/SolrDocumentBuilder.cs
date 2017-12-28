using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Utilities;
using Sitecore.Data.Items;
using Sitecore.Data.LanguageFallback;
using Sitecore.Data.Managers;
using Sitecore.Diagnostics;
using Sitecore.ContentSearch;
using System.Collections.Generic;
using Sitecore.Data;

namespace Sitecore.Support.ContentSearch.SolrProvider
{
  public class SolrDocumentBuilder : Sitecore.ContentSearch.SolrProvider.SolrDocumentBuilder
  {

    public SolrDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {

    }


    protected override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

        if (this.Options.IndexAllFields)
        {
          this.Indexable.LoadAllFields();
        }

        var processedFields = new HashSet<string>();

        if (IsParallel)
        {
          var exceptions = new ConcurrentQueue<Exception>();

          this.ParallelForeachProxy.ForEach(this.Indexable.Fields, this.ParallelOptions, f =>
          {
            try
            {
              processedFields.Add(f.Id.ToString());
              this.CheckAndAddField(this.Indexable, f);
            }
            catch (Exception ex)
            {
              exceptions.Enqueue(ex);
            }
          });

          if (exceptions.Count > 0)
          {
            throw new AggregateException(exceptions);
          }

          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
            var includedFields = new HashSet<string>(this.Options.IncludedFields);

            includedFields.ExceptWith(processedFields);

            this.ParallelForeachProxy.ForEach(includedFields, this.ParallelOptions, fieldId =>
            {
              try
              {
                ID id;
                if (ID.TryParse(fieldId, out id))
                {
                  var field = this.Indexable.GetFieldById(id);
                  if (field != null)
                  {
                    this.CheckAndAddField(this.Indexable, field);
                  }
                }
              }
              catch (Exception ex)
              {
                exceptions.Enqueue(ex);
              }
            });
          }
        }
        else
        {
          foreach (var field in this.Indexable.Fields)
          {
            processedFields.Add(field.Id.ToString());
            this.CheckAndAddField(this.Indexable, field);
          }

          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
            var includedFields = new HashSet<string>(this.Options.IncludedFields);

            includedFields.ExceptWith(processedFields);

            foreach (var fieldId in includedFields)
            {
              ID id;
              if (ID.TryParse(fieldId, out id))
              {
                var field = this.Indexable.GetFieldById(id);
                if (field != null)
                {
                  this.CheckAndAddField(this.Indexable, field);
                }
              }
            }
          }
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }
  }
}