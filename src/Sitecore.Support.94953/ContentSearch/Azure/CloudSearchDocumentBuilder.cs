
namespace Sitecore.Support.ContentSearch.Azure
{
  using System;
  using System.Collections.Concurrent;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Diagnostics;
  using StringExtensions;

  public class CloudSearchDocumentBuilder : Sitecore.ContentSearch.Azure.CloudSearchDocumentBuilder
  {

    public CloudSearchDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {

    }

    protected override void AddField(IIndexableDataField field)
    {
      if (field.Name.IsNullOrEmpty())
      {
        CrawlingLog.Log.Warn("[Index={0}] '{1}' field of '{2}' item is skipped: the field name is missed."
          .FormatWith(this.Index.Name, field.Id, this.Indexable.Id));

        return;
      }

      base.AddField(field);
    }

    protected override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

        if (this.Options.IndexAllFields)
        {
          this.AddItemFieldsByItemList();
        }
        else
        {
          this.AddItemFieldsByIncludeList();
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }

    protected virtual void AddItemFieldsByItemList()
    {
      this.Indexable.LoadAllFields();

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
      }
      else
      {
        foreach (var field in this.Indexable.Fields)
        {
          this.CheckAndAddField(this.Indexable, field);
        }
      }
    }

    protected virtual void AddItemFieldsByIncludeList()
    {
      
    }
  }
}