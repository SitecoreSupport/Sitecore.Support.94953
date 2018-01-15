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
using System.Linq;

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

        var loadedFields = new HashSet<string>(Indexable.Fields.Select(f => f.Id.ToString()));
        var includedFields = new HashSet<string>();
        if (this.Options.HasIncludedFields)
        {
          includedFields = new HashSet<string>(this.Options.IncludedFields);
        }
        includedFields.ExceptWith(loadedFields);

        if (IsParallel)
        {
          var exceptions = new ConcurrentQueue<Exception>();

          this.ParallelForeachProxy.ForEach(this.Indexable.Fields, this.ParallelOptions, f =>
          {
            try
            {
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
            this.CheckAndAddField(this.Indexable, field);
          }

          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
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