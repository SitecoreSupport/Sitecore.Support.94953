// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SolrDocumentBuilder.cs" company="Sitecore">
//   Copyright (c) Sitecore. All rights reserved.
// </copyright>
// <summary>
//   Defines the SolrDocumentBuilder type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Sitecore.ContentSearch.Abstractions;
using Sitecore.Diagnostics;
using Sitecore.ContentSearch.Linq;

using SolrNet.Mapping.Validation.Rules;

namespace Sitecore.Support.ContentSearch.SolrProvider
{
  using System;
  using System.Collections.Concurrent;
  using System.Collections.Generic;
  using System.Diagnostics;
  using System.Globalization;
  using System.Linq;

  using Sitecore.Configuration;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Boosting;
  using Sitecore.ContentSearch.Diagnostics;
  using Sitecore.ContentSearch.SolrProvider;
  using Sitecore.Data.Fields;
  using Sitecore.Data.Items;
  using BuiltinFields = Search.BuiltinFields;

  /// <summary>
  /// The solr document builder.
  /// </summary>
  public class SolrDocumentBuilder : AbstractDocumentBuilder<ConcurrentDictionary<string, object>>
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
    public SolrDocumentBuilder(IIndexable indexable, IProviderUpdateContext context)
        : base(indexable, context)
    {
      this.Context = context;
      this.fieldNameTranslator = context.Index.FieldNameTranslator as SolrFieldNameTranslator;
      this.culture = indexable.Culture;
      this.settings = context.Index.Locator.GetInstance<ISettings>();
    }

    /// <summary>
    /// Adds the field.
    /// </summary>
    /// <param name="fieldName">Name of the field.</param>
    /// <param name="fieldValue">The field value.</param>
    /// <param name="append">if set to <c>true</c> [appends] to any existing field values.</param>
    public override void AddField(string fieldName, object fieldValue, bool append = false)
    {
      var fieldMap = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName);

      if (fieldMap != null)
      {
        fieldValue = fieldMap.FormatForWriting(fieldValue);
      }

      if (fieldValue == null)
      {
        VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field name:{0} - Value is empty.", fieldName));
        return;
      }

      //We do this before the fieldName is processed
      var boostAmount = this.GetFieldConfigurationBoost(fieldName);

      var translatedFieldName = this.fieldNameTranslator.GetIndexFieldName(fieldName, fieldValue.GetType(), this.culture);

      this.StoreField(fieldName, translatedFieldName, fieldValue, append, boostAmount);
    }

    /// <summary>
    /// Adds the field.
    /// </summary>
    /// <param name="field">The field.</param>
    public override void AddField(IIndexableDataField field)
    {
      var fieldName = field.Name;
      var fieldValue = this.Index.Configuration.FieldReaders.GetFieldValue(field);
      var fieldMap = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName);

      if (fieldMap != null && (fieldValue == null && (fieldMap as SolrSearchFieldConfiguration).NullValue != null))
      {
        fieldValue = (fieldMap as SolrSearchFieldConfiguration).NullValue;
      }

      if (fieldValue is string)
      {
        if (fieldMap != null && ((string)fieldValue == string.Empty && (fieldMap as SolrSearchFieldConfiguration).EmptyString != null))
        {
          fieldValue = (fieldMap as SolrSearchFieldConfiguration).EmptyString;
        }
      }

      if (fieldValue == null)
      {
        VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Value is null.", field.Id, field.Name, field.TypeKey));
        return;
      }

      if (string.IsNullOrEmpty(fieldValue.ToString()))
      {
        VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Value is empty.", field.Id, field.Name, field.TypeKey));
        return;
      }

      //Boost set in the UI for the field
      var clientBoost = BoostingManager.ResolveFieldBoosting(field);

      //Boost set in configuration
      var configBoost = this.GetFieldConfigurationBoost(fieldName);

      //TODO: Should one take precident over the other ? atm just adding together.
      var boost = clientBoost + configBoost;

      var translatedFieldName = this.fieldNameTranslator.GetIndexFieldName(fieldName, fieldValue.GetType(), this.culture);

      if (!this.IsMedia && IndexOperationsHelper.IsTextField(field))
      {
        this.StoreField(BuiltinFields.Content, BuiltinFields.Content, fieldValue, true, null, field.TypeKey);
      }

      this.StoreField(fieldName, translatedFieldName, fieldValue, boost: boost, returnType: field.TypeKey);
    }

    public override void AddBoost()
    {
      var boost = BoostingManager.ResolveItemBoosting(this.Indexable);

      if (boost > 0)
      {
        this.Document.GetOrAdd("_documentBoost", boost);
      }
    }

    public override void AddComputedIndexFields()
    {
      foreach (var computedIndexField in this.Options.ComputedIndexFields)
      {
        object fieldValue;

        try
        {
          fieldValue = computedIndexField.ComputeFieldValue(this.Indexable);
        }
        catch (Exception ex)
        {
          CrawlingLog.Log.Warn(string.Format("Could not compute value for ComputedIndexField: {0} for indexable: {1}", computedIndexField.FieldName, this.Indexable.UniqueId), ex);
          if (this.Settings.StopOnCrawlFieldError())
          {
            throw;
          }

          System.Diagnostics.Debug.WriteLine(ex);
          continue;
        }

        // Not in schema and someone has configured a specific return type
        if (!string.IsNullOrEmpty(computedIndexField.ReturnType) && !this.Index.Schema.AllFieldNames.Contains(computedIndexField.FieldName))
        {
          this.AddField(computedIndexField.FieldName, fieldValue, computedIndexField.ReturnType);
        }
        else
        {
          this.AddField(computedIndexField.FieldName, fieldValue, true);
        }
      }
    }

    private void AddField(string fieldName, object fieldValue, string returnType)
    {
      var fieldMap = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName);

      if (fieldMap != null && (fieldValue == null && (fieldMap as SolrSearchFieldConfiguration).NullValue != null))
      {
        //VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field name:{0}, returnType:{1} - Value is null.", fieldName, returnType));
        //return;
        fieldValue = (fieldMap as SolrSearchFieldConfiguration).NullValue;
      }

      if (fieldValue is string)
      {
        if (fieldMap != null && (fieldValue.ToString() == string.Empty && (fieldMap as SolrSearchFieldConfiguration).EmptyString != null))
        {

          //VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field name:{0}, returnType:{1} - Value is empty.", fieldName, returnType));
          //return;
          fieldValue = (fieldMap as SolrSearchFieldConfiguration).EmptyString;
        }
      }

      if (fieldValue == null)
      {
        VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field name:{0}, returnType:{1} - Value is null.", fieldName, returnType));
        return;
      }

      //Boost set in configuration
      var configBoost = this.GetFieldConfigurationBoost(fieldName);

      var translatedFieldName = this.fieldNameTranslator.GetIndexFieldName(fieldName, returnType, this.culture);

      this.StoreField(fieldName, translatedFieldName, fieldValue, false, configBoost, returnType);
    }

    private void StoreField(string unTranslatedFieldName, string fieldName, object fieldValue, bool append = false, float? boost = null, string returnType = null)
    {
      var unformattedValue = fieldValue;

      if (this.Index.Configuration.IndexFieldStorageValueFormatter != null)
      {
        fieldValue = this.Index.Configuration.IndexFieldStorageValueFormatter.FormatValueForIndexStorage(fieldValue, unTranslatedFieldName);
      }

      if (VerboseLogging.Enabled)
      {
        StringBuilder sb = new StringBuilder();
        sb.AppendFormat("Field: {0}" + Environment.NewLine, fieldName);
        sb.AppendFormat(" - value: {0}{1}" + Environment.NewLine, unformattedValue != null ? unformattedValue.GetType().ToString() : "NULL", !(unformattedValue is string) && (unformattedValue is IEnumerable) ? " - count : " + ((IEnumerable)unformattedValue).Cast<object>().Count() : "");
        sb.AppendFormat(" - unformatted value: {0}" + Environment.NewLine, unformattedValue ?? "NULL");
        sb.AppendFormat(" - formatted value:   {0}" + Environment.NewLine, fieldValue ?? "NULL");
        sb.AppendFormat(" - returnType: {0}" + Environment.NewLine, returnType);
        sb.AppendFormat(" - boost: {0}" + Environment.NewLine, boost);
        sb.AppendFormat(" - append: {0}" + Environment.NewLine, append);
        VerboseLogging.CrawlingLogDebug(sb.ToString);
      }

      if (append)
      {
        if (this.Document.ContainsKey(fieldName) && fieldValue is string)
        {
          this.Document[fieldName] += " " + (string)fieldValue;
        }
      }

      if (this.Document.ContainsKey(fieldName))
      {
        return;
      }

      if (boost != null && boost > 0)
      {
        fieldValue = new SolrBoostedField(fieldValue, boost);
      }

      this.Document.GetOrAdd(fieldName, fieldValue);

      // If field has culture specific text version
      if (this.fieldNameTranslator.HasCulture(fieldName) && !this.settings.DefaultLanguage().StartsWith(this.culture.TwoLetterISOLanguageName))
      {
        this.Document.GetOrAdd(this.fieldNameTranslator.StripKnownCultures(fieldName), fieldValue);
      }
    }

    private float GetFieldConfigurationBoost(string fieldName)
    {
      var fieldConfig = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName) as SolrSearchFieldConfiguration;

      if (fieldConfig != null)
      {
        return fieldConfig.Boost;
      }

      return 0;
    }
  }
}
